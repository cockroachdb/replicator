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

	// Create a source database connection.
	sourceFixture, cancel, err := sinktest.NewFixture()
	if !a.NoError(err) {
		return
	}
	defer cancel()

	sourceCtx := sourceFixture.Context

	// The target fixture contains the cdc-sink server.
	targetFixture, cancel, err := newTestFixture()
	if !a.NoError(err) {
		return
	}
	defer cancel()

	targetCtx := targetFixture.Context
	targetDB := targetFixture.TestDB.Ident()
	targetPool := targetFixture.Pool

	// Set up source and target tables.
	source, err := sourceFixture.CreateTable(sourceCtx, "CREATE TABLE %s (pk INT PRIMARY KEY, val STRING)")
	if !a.NoError(err) {
		return
	}

	// Since we're creating the target table without using the helper
	// CreateTable(), we need to manually refresh the target's Watcher.
	target := sinktest.NewTableInfo(targetFixture.DBInfo, ident.NewTable(targetDB, ident.Public, source.Name().Table()))
	if !a.NoError(target.Exec(targetCtx, "CREATE TABLE %s (pk INT PRIMARY KEY, val STRING)")) {
		return
	}
	a.NoError(targetFixture.Watcher.Refresh(targetCtx, targetPool))

	// Add base data to the source table.
	a.NoError(source.Exec(sourceCtx, "INSERT INTO %s (pk, val) VALUES (1, 'one')"))
	ct, err := source.RowCount(sourceCtx)
	a.NoError(err)
	a.Equal(1, ct)

	// Allow access.
	method, priv, err := jwt.InsertTestingKey(targetCtx, targetPool, targetFixture.Authenticator, targetFixture.StagingDB)
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
	// Set up the changefeed.
	var feedURL url.URL
	var createStmt string

	// No webhook_auth_header, so bake it into the query string.
	// See comments in cdc.Handler.ServeHTTP checkAccess.
	params.Set("access_token", token)
	feedURL = url.URL{
		Scheme:   "experimental-http",
		Host:     targetFixture.Listener.Addr().String(),
		Path:     path.Join(target.Name().Database().Raw(), target.Name().Schema().Raw()),
		RawQuery: params.Encode(),
	}
	createStmt = "CREATE CHANGEFEED FOR TABLE %s " +
		"INTO '" + feedURL.String() + "' " +
		"WITH updated,resolved='1s'"

	log.Debugf("changefeed URL is %s", feedURL.String())
	if !a.NoError(source.Exec(sourceCtx, createStmt)) {
		return
	}

	// Wait for the backfilled value.
	for {
		ct, err := target.RowCount(targetCtx)
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
	a.NoError(source.Exec(sourceCtx, "INSERT INTO %s (pk, val) VALUES (2, 'two')"))
	ct, err = source.RowCount(sourceCtx)
	a.NoError(err)
	a.Equal(2, ct)

	// Wait for the streamed value.
	for {
		ct, err := target.RowCount(targetCtx)
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

func testIntegrationWebhook(t *testing.T, immediate bool) {
	a := assert.New(t)

	var stopped <-chan struct{}
	defer func() {
		if stopped != nil {
			<-stopped
		}
	}()

	// Create a source database connection.
	sourceFixture, cancel, err := sinktest.NewFixture()
	if !a.NoError(err) {
		return
	}
	defer cancel()

	sourceCtx := sourceFixture.Context

	// The target fixture contains the cdc-sink server.
	targetFixture, cancel, err := newTestFixture()
	if !a.NoError(err) {
		return
	}
	defer cancel()

	targetCtx := targetFixture.Context
	targetDB := targetFixture.TestDB.Ident()
	targetPool := targetFixture.Pool
	useWebhook := targetFixture.Config.GenerateSelfSigned

	// Webhook is not supported so we can skip this test.
	if !useWebhook {
		return
	}

	// Set up source and target tables.
	source, err := sourceFixture.CreateTable(sourceCtx, "CREATE TABLE %s (pk INT PRIMARY KEY, val STRING)")
	if !a.NoError(err) {
		return
	}

	// Since we're creating the target table without using the helper
	// CreateTable(), we need to manually refresh the target's Watcher.
	target := sinktest.NewTableInfo(targetFixture.DBInfo, ident.NewTable(targetDB, ident.Public, source.Name().Table()))
	if !a.NoError(target.Exec(targetCtx, "CREATE TABLE %s (pk INT PRIMARY KEY, val STRING)")) {
		return
	}
	a.NoError(targetFixture.Watcher.Refresh(targetCtx, targetPool))

	// Add base data to the source table.
	a.NoError(source.Exec(sourceCtx, "INSERT INTO %s (pk, val) VALUES (1, 'one')"))
	ct, err := source.RowCount(sourceCtx)
	a.NoError(err)
	a.Equal(1, ct)

	// Allow access.
	method, priv, err := jwt.InsertTestingKey(targetCtx, targetPool, targetFixture.Authenticator, targetFixture.StagingDB)
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
	params.Set("insecure_tls_skip_verify", "true")

	// Set up the changefeed.
	var feedURL url.URL
	var createStmt string
	feedURL = url.URL{
		Scheme:   "webhook-https",
		Host:     targetFixture.Listener.Addr().String(),
		Path:     path.Join(target.Name().Database().Raw(), target.Name().Schema().Raw()),
		RawQuery: params.Encode(),
	}
	createStmt = "CREATE CHANGEFEED FOR TABLE %s " +
		"INTO '" + feedURL.String() + "' " +
		"WITH updated," +
		"     resolved='1s'," +
		"     webhook_auth_header='Bearer " + token + "'"

	log.Debugf("changefeed URL is %s", feedURL.String())
	if !a.NoError(source.Exec(sourceCtx, createStmt)) {
		return
	}

	// Wait for the backfilled value.
	for {
		ct, err := target.RowCount(targetCtx)
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
	a.NoError(source.Exec(sourceCtx, "INSERT INTO %s (pk, val) VALUES (2, 'two')"))
	ct, err = source.RowCount(sourceCtx)
	a.NoError(err)
	a.Equal(2, ct)

	// Wait for the streamed value.
	for {
		ct, err := target.RowCount(targetCtx)
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
