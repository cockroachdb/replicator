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
	"net/url"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/source/cdc"
	"github.com/cockroachdb/cdc-sink/internal/target/auth/jwt"
	"github.com/cockroachdb/cdc-sink/internal/target/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	joonix "github.com/joonix/log"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
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
	t.Run("deferred", func(t *testing.T) { testIntegration(t, false) })
	t.Run("immediate", func(t *testing.T) { testIntegration(t, true) })
}

func testIntegration(t *testing.T, immediate bool) {
	a := assert.New(t)

	var stopped <-chan struct{}
	defer func() {
		if stopped != nil {
			<-stopped
		}
	}()

	ctx, dbInfo, cancel := sinktest.Context()
	defer cancel()

	sourceDB, cancel, err := sinktest.CreateDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	targetDB, cancel, err := sinktest.CreateDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	// Prefer the webhook format for current versions of CRDB.
	useWebhook := true
	if strings.Contains(dbInfo.Version(), "v20.2.") || strings.Contains(dbInfo.Version(), "v21.1.") {
		useWebhook = false
	}

	*GenerateSelfSigned = useWebhook
	defer func() { *GenerateSelfSigned = false }()

	// Pick an ephemeral port.
	srv, stopped, err := newServer(ctx, "127.0.0.1:0", dbInfo.Pool().Config().ConnString())
	if !a.NoError(err) {
		return
	}
	// Run the server loop in the background.
	go srv.serve()

	// Set up source and target tables.
	source, err := sinktest.CreateTable(ctx, sourceDB, "CREATE TABLE %s (pk INT PRIMARY KEY, val STRING)")
	if !a.NoError(err) {
		return
	}

	target := sinktest.NewTableInfo(dbInfo, ident.NewTable(targetDB, ident.Public, source.Name().Table()))
	if !a.NoError(target.Exec(ctx, "CREATE TABLE %s (pk INT PRIMARY KEY, val STRING)")) {
		return
	}

	// Add base data to the source table.
	a.NoError(source.Exec(ctx, "INSERT INTO %s (pk, val) VALUES (1, 'one')"))
	ct, err := source.RowCount(ctx)
	a.NoError(err)
	a.Equal(1, ct)

	// Allow access.
	method, priv, err := jwt.InsertTestingKey(ctx, dbInfo.Pool(), srv.authenticator, ident.StagingDB)
	defer cancel()
	if !a.NoError(err) {
		return
	}
	_, token, err := jwt.Sign(method, priv, []ident.Schema{target.Name().AsSchema()})
	if !a.NoError(err) {
		return
	}

	params := make(url.Values)
	if immediate {
		params.Set(cdc.ImmediateParam, "true")
	}
	if useWebhook {
		params.Set("insecure_tls_skip_verify", "true")
	}
	// Set up the changefeed.
	var feedURL url.URL
	var createStmt string
	if useWebhook {
		feedURL = url.URL{
			Scheme:   "webhook-https",
			Host:     srv.listener.Addr().String(),
			Path:     path.Join(target.Name().Database().Raw(), target.Name().Schema().Raw()),
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
			Host:     srv.listener.Addr().String(),
			Path:     path.Join(target.Name().Database().Raw(), target.Name().Schema().Raw()),
			RawQuery: params.Encode(),
		}
		createStmt = "CREATE CHANGEFEED FOR TABLE %s " +
			"INTO '" + feedURL.String() + "' " +
			"WITH updated,resolved='1s'"
	}
	log.Debugf("changefeed URL is %s", feedURL.String())
	if !a.NoError(source.Exec(ctx, createStmt)) {
		return
	}

	// Wait for the backfilled value.
	for {
		ct, err := target.RowCount(ctx)
		if !a.NoError(err) {
			return
		}
		if ct >= 1 {
			break
		}
		log.Debug("waiting for backfill")
		time.Sleep(time.Second)
	}

	// Insert an additional value
	a.NoError(source.Exec(ctx, "INSERT INTO %s (pk, val) VALUES (2, 'two')"))
	ct, err = source.RowCount(ctx)
	a.NoError(err)
	a.Equal(2, ct)

	// Wait for the streamed value.
	for {
		ct, err := target.RowCount(ctx)
		if !a.NoError(err) {
			return
		}
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
