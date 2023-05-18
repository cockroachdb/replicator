// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"fmt"
	"net/url"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/sinktest/base"
	"github.com/cockroachdb/cdc-sink/internal/source/cdc"
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	jwtAuth "github.com/cockroachdb/cdc-sink/internal/staging/auth/jwt"
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

	supportsWebhook := supportsWebhook(sourceFixture.DBInfo)
	if webhook && !supportsWebhook {
		t.Skipf("Webhook is not compatible with %s version of cockroach.", sourceFixture.DBInfo.Version())
	}

	ctx := sourceFixture.Context

	// Create a basic destination database connection.
	destFixture, cancel, err := base.NewFixture()
	r.NoError(err)
	defer cancel()

	targetDB := destFixture.TestDB.Ident()
	targetPool := destFixture.Pool

	// The target fixture contains the cdc-sink server.
	targetFixture, cancel, err := newTestFixture(ctx, &Config{
		CDC: cdc.Config{
			BaseConfig: logical.BaseConfig{
				Immediate:  immediate,
				LoopName:   "changefeed",
				StagingDB:  destFixture.StagingDB.Ident(),
				TargetDB:   destFixture.TestDB.Ident(),
				TargetConn: destFixture.Pool.Config().ConnString(),
			},
			MetaTableName: ident.New("resolved_timestamps"),
		},
		BindAddr:           "127.0.0.1:0",
		GenerateSelfSigned: webhook && supportsWebhook, // Webhook implies self-signed TLS is ok.
	})
	r.NoError(err)
	defer cancel()

	// Set up source and target tables.
	source, err := sourceFixture.CreateTable(ctx, "CREATE TABLE %s (pk INT PRIMARY KEY, val STRING)")
	r.NoError(err)

	// Since we're creating the target table without using the helper
	// CreateTable(), we need to manually refresh the target's Watcher.
	target := ident.NewTable(targetDB, ident.Public, source.Name().Table())
	_, err = targetPool.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (pk INT PRIMARY KEY, val STRING)", target))
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
	method, priv, err := jwtAuth.InsertTestingKey(ctx, targetPool, targetFixture.Authenticator, targetFixture.StagingDB)
	r.NoError(err)

	_, token, err := jwtAuth.Sign(method, priv, []ident.Schema{target.AsSchema()})
	r.NoError(err)

	params := make(url.Values)
	// Set up the changefeed.
	var feedURL url.URL
	var createStmt string
	if webhook {
		params.Set("insecure_tls_skip_verify", "true")
		feedURL = url.URL{
			Scheme:   "webhook-https",
			Host:     targetFixture.Listener.Addr().String(),
			Path:     path.Join(target.Database().Raw(), target.Schema().Raw()),
			RawQuery: params.Encode(),
		}
		createStmt = "CREATE CHANGEFEED FOR TABLE %s " +
			"INTO '" + feedURL.String() + "' " +
			"WITH updated," +
			"     resolved='1s'," +
			"     webhook_auth_header='Bearer " + token + "'"
	} else {
		// No webhook_auth_header, so bake it into the query string.
		// See comments in cdc.Handler.ServeHTTP checkAccess.
		params.Set("access_token", token)
		feedURL = url.URL{
			Scheme:   "experimental-http",
			Host:     targetFixture.Listener.Addr().String(),
			Path:     path.Join(target.Database().Raw(), target.Schema().Raw()),
			RawQuery: params.Encode(),
		}
		createStmt = "CREATE CHANGEFEED FOR TABLE %s " +
			"INTO '" + feedURL.String() + "' " +
			"WITH updated,resolved='1s'"
	}
	// Don't wait the entire 30s. This options was introduced in the
	// same versions as webhooks.
	if supportsMinCheckpoint(sourceFixture.DBInfo) {
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
	log.WithField("metrics", metrics).Debug()
}

func supportsMinCheckpoint(dbInfo *base.DBInfo) bool {
	if strings.Contains(dbInfo.Version(), "v20.") || strings.Contains(dbInfo.Version(), "v21.") {
		return false
	}
	return true
}

func supportsWebhook(dbInfo *base.DBInfo) bool {
	// In older versions of CRDB, the webhook endpoint is not available so no
	// self signed certificate is needed. This acts as a signal as to wether the
	// webhook endpoint is available.
	if strings.Contains(dbInfo.Version(), "v20.2.") || strings.Contains(dbInfo.Version(), "v21.1.") {
		return false
	}
	return true
}
