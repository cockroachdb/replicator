// Copyright 2023 The Cockroach Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/base"
	"github.com/cockroachdb/cdc-sink/internal/source/cdc"
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	jwtAuth "github.com/cockroachdb/cdc-sink/internal/staging/auth/jwt"
	"github.com/cockroachdb/cdc-sink/internal/util/diag"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	joonix "github.com/joonix/log"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	f := joonix.NewFormatter()
	f.PrettyPrint = true
	log.SetFormatter(f)
	log.Exit(m.Run())
}

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("short tests requested")
	}
	t.Run("deferred_http", func(t *testing.T) { testIntegration(t, false, false) })
	t.Run("deferred_webhook", func(t *testing.T) { testIntegration(t, false, true) })
	t.Run("immediate_http", func(t *testing.T) { testIntegration(t, true, false) })
	t.Run("immediate_webhook", func(t *testing.T) { testIntegration(t, true, true) })
}

func testIntegration(t *testing.T, immediate bool, webhook bool) {
	log.SetLevel(log.TraceLevel)
	a := assert.New(t)
	r := require.New(t)

	var stopped <-chan struct{}
	defer func() {
		if stopped != nil {
			<-stopped
		}
	}()

	// Create a basic fixture to represent a source database.
	sourceFixture, cancel, err := base.NewFixture()
	r.NoError(err)
	defer cancel()

	supportsWebhook := supportsWebhook(sourceFixture.TargetPool.Version)
	if webhook && !supportsWebhook {
		t.Skipf("Webhook is not compatible with %s version of cockroach.", sourceFixture.TargetPool.Version)
	}

	ctx := sourceFixture.Context

	// Create a basic destination database connection.
	destFixture, cancel, err := base.NewFixture()
	r.NoError(err)
	defer cancel()

	targetDB := destFixture.TargetSchema.Schema()
	targetPool := destFixture.TargetPool

	// The target fixture contains the cdc-sink server.
	targetFixture, cancel, err := newTestFixture(ctx, &Config{
		CDC: cdc.Config{
			BaseConfig: logical.BaseConfig{
				Immediate:     immediate,
				StagingConn:   destFixture.StagingPool.ConnectionString,
				StagingSchema: destFixture.StagingDB.Schema(),
				TargetConn:    destFixture.TargetPool.ConnectionString,
			},
			MetaTableName: ident.New("resolved_timestamps"),
		},
		BindAddr:           "127.0.0.1:0",
		GenerateSelfSigned: webhook && supportsWebhook, // Webhook implies self-signed TLS is ok.
	})
	r.NoError(err)
	defer cancel()

	// Set up source and target tables.
	source, err := sourceFixture.CreateSourceTable(ctx, "CREATE TABLE %s (pk INT PRIMARY KEY, val STRING)")
	r.NoError(err)

	// Since we're creating the target table without using the helper
	// CreateTable(), we need to manually refresh the target's Watcher.
	target := ident.NewTable(targetDB, source.Name().Table())
	_, err = targetPool.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (pk INT PRIMARY KEY, val VARCHAR(2048))", target))
	r.NoError(err)
	watcher, err := targetFixture.Watcher.Get(ctx, targetDB)
	r.NoError(err)
	r.NoError(watcher.Refresh(ctx, targetPool))

	// Add base data to the source table.
	r.NoError(source.Exec(ctx, "INSERT INTO %s (pk, val) VALUES (1, 'one')"))
	ct, err := source.RowCount(ctx)
	r.NoError(err)
	a.Equal(1, ct)

	// Allow access.
	method, priv, err := jwtAuth.InsertTestingKey(ctx, targetFixture.StagingPool, targetFixture.Authenticator, targetFixture.StagingDB)
	r.NoError(err)

	_, token, err := jwtAuth.Sign(method, priv, []ident.Schema{target.Schema(), diag.Schema})
	r.NoError(err)

	params := make(url.Values)
	// Set up the changefeed.
	var diagURL, feedURL url.URL
	var createStmt string
	if webhook {
		params.Set("insecure_tls_skip_verify", "true")
		feedURL = url.URL{
			Scheme:   "webhook-https",
			Host:     targetFixture.Listener.Addr().String(),
			Path:     ident.Join(target.Schema(), ident.Raw, '/'),
			RawQuery: params.Encode(),
		}
		createStmt = "CREATE CHANGEFEED FOR TABLE %s " +
			"INTO '" + feedURL.String() + "' " +
			"WITH updated," +
			"     resolved='1s'," +
			"     webhook_auth_header='Bearer " + token + "'"

		diagURL = url.URL{
			Scheme:   "https",
			Host:     targetFixture.Listener.Addr().String(),
			Path:     "/_/diag",
			RawQuery: "access_token=" + token,
		}
	} else {
		// No webhook_auth_header, so bake it into the query string.
		// See comments in cdc.Handler.ServeHTTP checkAccess.
		params.Set("access_token", token)
		feedURL = url.URL{
			Scheme:   "experimental-http",
			Host:     targetFixture.Listener.Addr().String(),
			Path:     ident.Join(target.Schema(), ident.Raw, '/'),
			RawQuery: params.Encode(),
		}
		createStmt = "CREATE CHANGEFEED FOR TABLE %s " +
			"INTO '" + feedURL.String() + "' " +
			"WITH updated,resolved='1s'"

		diagURL = url.URL{
			Scheme:   "http",
			Host:     targetFixture.Listener.Addr().String(),
			Path:     "/_/diag",
			RawQuery: "access_token=" + token,
		}
	}

	// Don't wait the entire 30s. This options was introduced in the
	// same versions as webhooks.
	if supportsMinCheckpoint(sourceFixture.TargetPool.Version) {
		createStmt += ",min_checkpoint_frequency='1s'"
	}
	log.Debugf("changefeed URL is %s", feedURL.String())
	r.NoError(source.Exec(ctx, createStmt))

	// Wait for the backfilled value.
	for {
		ct, err := base.GetRowCount(ctx, targetPool, target)
		r.NoError(err)
		if ct >= 1 {
			break
		}
		log.Debug("waiting for backfill")
		time.Sleep(time.Second)
	}

	// Insert an additional value
	r.NoError(source.Exec(ctx, "INSERT INTO %s (pk, val) VALUES (2, 'two')"))
	ct, err = source.RowCount(ctx)
	r.NoError(err)
	a.Equal(2, ct)

	// Wait for the streamed value.
	for {
		ct, err := base.GetRowCount(ctx, targetPool, target)
		r.NoError(err)
		if ct >= 2 {
			break
		}
		log.Debug("waiting for stream")
		time.Sleep(time.Second)
	}

	metrics, err := prometheus.DefaultGatherer.Gather()
	a.NoError(err)
	log.WithField("metrics", metrics).Trace()

	sinktest.CheckDiagnostics(ctx, t, targetFixture.Diagnostics)

	// Ensure that diagnostic endpoint is protected, since it has
	// potentially-sensitive connect strings.
	t.Run("check diag endpoint", func(t *testing.T) {
		a := assert.New(t)
		client := &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
		}
		u := diagURL.String()
		resp, err := client.Get(u)
		if a.NoError(err) {
			a.Equal(http.StatusOK, resp.StatusCode, u)
			a.Equal("application/json", resp.Header.Get("content-type"))
			buf, _ := io.ReadAll(resp.Body)
			t.Log(string(buf))
		}

		// Remove auth info.
		diagURL.RawQuery = ""
		u = diagURL.String()
		resp, err = client.Get(u)
		if a.NoError(err) {
			a.Equal(http.StatusForbidden, resp.StatusCode, u)
		}
	})
}

func supportsMinCheckpoint(version string) bool {
	if strings.Contains(version, "v20.") || strings.Contains(version, "v21.") {
		return false
	}
	return true
}

func supportsWebhook(version string) bool {
	// In older versions of CRDB, the webhook endpoint is not available so no
	// self signed certificate is needed. This acts as a signal as to wether the
	// webhook endpoint is available.
	if strings.Contains(version, "v20.2.") || strings.Contains(version, "v21.1.") {
		return false
	}
	return true
}
