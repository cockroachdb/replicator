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

	"github.com/cockroachdb/cdc-sink/internal/sequencer"
	stagingProd "github.com/cockroachdb/cdc-sink/internal/sinkprod"
	"github.com/cockroachdb/cdc-sink/internal/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/base"
	"github.com/cockroachdb/cdc-sink/internal/source/cdc"
	jwtAuth "github.com/cockroachdb/cdc-sink/internal/util/auth/jwt"
	"github.com/cockroachdb/cdc-sink/internal/util/diag"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stdlogical"
	"github.com/cockroachdb/cdc-sink/internal/util/stdserver"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
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

// These constants are used to create test permutations.
const (
	testModeDiff = 1 << iota
	testModeImmediate
	testModeQueries
	testModeParallel
	testModeWebhook

	testModeMax // Sentinel value
)

type testConfig struct {
	diff      bool
	immediate bool
	parallel  bool
	queries   bool
	webhook   bool
}

func (c *testConfig) String() string {
	var sb strings.Builder
	if c.diff {
		sb.WriteString(" diff")
	} else {
		sb.WriteString(" snapshot")
	}
	if c.immediate {
		sb.WriteString(" immediate")
	} else {
		sb.WriteString(" transactional")
	}
	if c.queries {
		sb.WriteString(" queries")
	} else {
		sb.WriteString(" tables")
	}
	if c.parallel {
		sb.WriteString(" parallel")
	} else {
		sb.WriteString(" serial")
	}
	if c.webhook {
		sb.WriteString(" webhook")
	} else {
		sb.WriteString(" bulk")
	}
	return sb.String()[1:]
}

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("short tests requested")
	}

	// Create all testing permutations. The test helper will skip in
	// cases that don't apply to a particular target.
	tcs := make([]testConfig, testModeMax)
	for i := range tcs {
		tcs[i] = testConfig{
			diff:      i&testModeDiff == testModeDiff,
			immediate: i&testModeImmediate == testModeImmediate,
			queries:   i&testModeQueries == testModeQueries,
			parallel:  i&testModeParallel == testModeParallel,
			webhook:   i&testModeWebhook == testModeWebhook,
		}
	}

	for _, tc := range tcs {
		t.Run(tc.String(), func(t *testing.T) {
			testIntegration(t, tc)
		})
	}
}

func testIntegration(t *testing.T, cfg testConfig) {
	t.Parallel()
	a := assert.New(t)
	r := require.New(t)

	var stopped <-chan struct{}
	defer func() {
		if stopped != nil {
			<-stopped
		}
	}()

	// Create a basic fixture to represent a source database.
	sourceFixture, err := base.NewFixture(t)
	r.NoError(err)

	version := sourceFixture.SourcePool.Version
	supportsWebhook := supportsWebhook(version)
	if cfg.webhook && !supportsWebhook {
		t.Skipf("Webhook is not compatible with %s version of cockroach.", version)
	}
	if cfg.queries && !supportsQueries(version) {
		t.Skipf("CDC queries are not compatible with %s version of cockroach", version)
	}

	ctx := sourceFixture.Context

	// Create a basic destination database connection.
	destFixture, err := base.NewFixture(t)
	r.NoError(err)

	targetDB := destFixture.TargetSchema.Schema()
	targetPool := destFixture.TargetPool

	serverCfg := &Config{
		CDC: cdc.Config{
			SequencerConfig: sequencer.Config{
				RetireOffset: time.Hour, // Allow post-hoc inspection of staged data.
			},
			Immediate: cfg.immediate,
		},
		HTTP: stdserver.Config{
			BindAddr:           "127.0.0.1:0",
			GenerateSelfSigned: cfg.webhook && supportsWebhook, // Webhook implies self-signed TLS is ok.
		},
		Staging: stagingProd.StagingConfig{
			CommonConfig: stagingProd.CommonConfig{
				Conn:        destFixture.StagingPool.ConnectionString,
				MaxPoolSize: 16,
			},
			Schema: destFixture.StagingDB.Schema(),
		},
		Target: stagingProd.TargetConfig{
			CommonConfig: stagingProd.CommonConfig{
				Conn:        targetPool.ConnectionString,
				MaxPoolSize: 16,
			},
		},
	}
	if cfg.parallel {
		serverCfg.CDC.SequencerConfig.Parallelism = 4
	} else {
		serverCfg.CDC.SequencerConfig.Parallelism = 1
	}
	r.NoError(serverCfg.Preflight())

	// The target fixture contains the cdc-sink server.
	targetFixture, cancel, err := newTestFixture(stopper.WithContext(ctx), serverCfg)
	r.NoError(err)
	defer cancel()
	// This is normally taken care of by stdlogical.Command.
	stdlogical.AddHandlers(targetFixture.Authenticator, targetFixture.Server.GetServeMux(), targetFixture.Diagnostics)

	// Set up source and target tables.
	source, err := sourceFixture.CreateSourceTable(ctx, "CREATE TABLE %s (pk INT PRIMARY KEY, val STRING)")
	r.NoError(err)

	// Since we're creating the target table without using the helper
	// CreateTable(), we need to manually refresh the target's Watcher.
	target := ident.NewTable(targetDB, source.Name().Table())
	_, err = targetPool.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (pk INT PRIMARY KEY, val VARCHAR(2048))", target))
	r.NoError(err)
	watcher, err := targetFixture.Watcher.Get(targetDB)
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
	var pathIdent ident.Identifier
	createStmt := "CREATE CHANGEFEED"
	if cfg.queries {
		pathIdent = target
	} else {
		pathIdent = target.Schema()
		createStmt += " FOR TABLE %s"
	}
	if cfg.webhook {
		params.Set("insecure_tls_skip_verify", "true")
		feedURL = url.URL{
			Scheme:   "webhook-https",
			Host:     targetFixture.Listener.Addr().String(),
			Path:     ident.Join(pathIdent, ident.Raw, '/'),
			RawQuery: params.Encode(),
		}
		createStmt += " INTO '" + feedURL.String() + "' " +
			" WITH updated," +
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
			Path:     ident.Join(pathIdent, ident.Raw, '/'),
			RawQuery: params.Encode(),
		}
		createStmt += " INTO '" + feedURL.String() + "' " +
			"WITH updated,resolved='1s'"

		diagURL = url.URL{
			Scheme:   "http",
			Host:     targetFixture.Listener.Addr().String(),
			Path:     "/_/diag",
			RawQuery: "access_token=" + token,
		}
	}
	if cfg.diff {
		createStmt += ",diff"
	}
	// Don't wait the entire 30s. This options was introduced in the
	// same versions as webhooks.
	if supportsMinCheckpoint(sourceFixture.TargetPool.Version) {
		createStmt += ",min_checkpoint_frequency='1s'"
	}
	if cfg.queries {
		createStmt += ",envelope='wrapped',format='json'"
		createStmt += " AS SELECT pk, val"
		createStmt += " FROM %s"
	}

	log.Debugf("changefeed URL is %s", feedURL.String())
	r.NoError(source.Exec(ctx, createStmt), createStmt)

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

	// Update the first value
	r.NoError(source.Exec(ctx, "UPSERT INTO %s (pk, val) VALUES (1, 'updated')"))

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
		time.Sleep(100 * time.Millisecond)
	}

	// Also wait to see that the update was applied.
	for {
		var val string
		r.NoError(targetPool.QueryRowContext(ctx,
			fmt.Sprintf("SELECT val FROM %s WHERE pk = 1", target),
		).Scan(&val))
		if val == "updated" {
			break
		}
		log.Debug("waiting for update")
		time.Sleep(100 * time.Millisecond)
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
			buf, err := io.ReadAll(resp.Body)
			a.NoError(err)
			a.NotEmpty(buf)
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

func supportsQueries(version string) bool {
	// While queries are supported in v22.2, they were in preview.
	if strings.Contains(version, "v20.") ||
		strings.Contains(version, "v21.") ||
		strings.Contains(version, "v22.") {
		return false
	}
	return true
}
