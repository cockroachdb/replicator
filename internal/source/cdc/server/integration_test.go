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
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/conveyor"
	"github.com/cockroachdb/replicator/internal/sequencer"
	stagingProd "github.com/cockroachdb/replicator/internal/sinkprod"
	"github.com/cockroachdb/replicator/internal/sinktest"
	"github.com/cockroachdb/replicator/internal/sinktest/all"
	"github.com/cockroachdb/replicator/internal/sinktest/base"
	"github.com/cockroachdb/replicator/internal/source/cdc"
	"github.com/cockroachdb/replicator/internal/types"
	jwtAuth "github.com/cockroachdb/replicator/internal/util/auth/jwt"
	"github.com/cockroachdb/replicator/internal/util/diag"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/stdlogical"
	"github.com/cockroachdb/replicator/internal/util/stdpool"
	"github.com/cockroachdb/replicator/internal/util/stdserver"
	"github.com/cockroachdb/replicator/internal/util/workload"
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

	sourceVersion := sourceFixture.SourcePool.Version
	supportsWebhook, err := supportsWebhook(sourceVersion)
	r.NoError(err)
	if cfg.webhook && !supportsWebhook {
		t.Skipf("Webhook is not compatible with %s version of cockroach.", sourceVersion)
	}
	supportsQueries, err := supportsQueries(sourceVersion)
	r.NoError(err)
	if cfg.queries && !supportsQueries {
		t.Skipf("CDC queries are not compatible with %s version of cockroach", sourceVersion)
	}

	ctx := sourceFixture.Context

	// Create a basic destination database connection.
	destFixture, err := base.NewFixture(t)
	r.NoError(err)

	targetDB := destFixture.TargetSchema.Schema()
	targetPool := destFixture.TargetPool

	serverCfg := &Config{
		CDC: cdc.Config{
			ConveyorConfig: conveyor.Config{
				Immediate: cfg.immediate,
			},
			SequencerConfig: sequencer.Config{
				RetireOffset: time.Hour, // Allow post-hoc inspection of staged data.
			},
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

	// The target fixture contains the Replicator server.
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
	if ok, err := supportsMinCheckpoint(sourceVersion); a.NoError(err) && ok {
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

// Necessary for faster resolved timestamps.
func supportsMinCheckpoint(version string) (bool, error) {
	return stdpool.CockroachMinVersion(version, "v22.1")
}

// While queries are supported in v22.2, they were in preview.
func supportsQueries(version string) (bool, error) {
	return stdpool.CockroachMinVersion(version, "v23.1")
}

// In older versions of CRDB, the webhook endpoint is not available so
// no self-signed certificate is needed. This acts as a signal whether
// the webhook endpoint is available.
func supportsWebhook(version string) (bool, error) {
	return stdpool.CockroachMinVersion(version, "v21.2")
}

func getConfig(
	cfg *testConfig, fixture *all.Fixture, targetPool *types.TargetPool,
) (*Config, error) {
	fmt.Println("target pool schema: ", targetPool.ConnectionString)
	return &Config{
		CDC: cdc.Config{
			ConveyorConfig: conveyor.Config{
				Immediate: cfg.immediate,
			},
			SequencerConfig: sequencer.Config{
				RetireOffset: time.Hour, // Allow post-hoc inspection of staged data.
				Parallelism:  1,
			},
			NDJsonBuffer: 1,
		},
		HTTP: stdserver.Config{
			BindAddr:           "127.0.0.1:0",
			GenerateSelfSigned: cfg.webhook, // Webhook implies self-signed TLS is ok.
		},
		Staging: stagingProd.StagingConfig{
			CommonConfig: stagingProd.CommonConfig{
				Conn:        fixture.StagingPool.ConnectionString,
				MaxPoolSize: 16,
			},
			Schema: fixture.StagingDB.Schema(),
		},
		Target: stagingProd.TargetConfig{
			CommonConfig: stagingProd.CommonConfig{
				Conn:        targetPool.ConnectionString,
				MaxPoolSize: 16,
			},
		},
	}, nil
}

const maxIterations = 25

func TestWorkload(t *testing.T) {
	testWorkload(t)
}

func testWorkload(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	r := require.New(t)

	targetFixture, err := all.NewFixture(t)
	r.NoError(err)

	sourceFixture, err := all.NewFixtureFromBase(targetFixture.Swapped())
	r.NoError(err)

	ctx := targetFixture.Context
	// These generators will act as sources of mutations to apply later
	// on and will then be used to validate the information in the
	// target.
	targetChecker, _, err := targetFixture.NewWorkload(ctx, &all.WorkloadConfig{})
	r.NoError(err)

	// Create the source generator workload.
	sourceSchema := targetFixture.SourceSchema.Schema()
	parent := ident.NewTable(sourceSchema, targetChecker.Parent.Name().Table())
	child := ident.NewTable(sourceSchema, targetChecker.Child.Name().Table())
	sourceGeneratorWorkload := workload.NewGeneratorBase(parent, child)
	r.NoError(err)
	cfg := &testConfig{webhook: true}
	serverCfg, err := getConfig(cfg, sourceFixture, targetFixture.TargetPool)
	r.NoError(err)

	// This sets default values that are not set in the testConfig.
	r.NoError(serverCfg.Preflight())

	// Create the tables on the target side to match.

	targetSchema := targetFixture.TargetSchema.Schema()
	sourcePool := targetFixture.SourcePool

	// TODO: debug this because we need to create the target schema tables
	// instead of putting it in the source.
	parent = sourceGeneratorWorkload.Parent
	child = sourceGeneratorWorkload.Child
	parentSQL, childSQL := all.WorkloadSchema(
		&all.WorkloadConfig{}, types.ProductPostgreSQL,
		parent, child)
	_, err = sourcePool.ExecContext(ctx, parentSQL)
	r.NoError(err)
	_, err = sourcePool.ExecContext(ctx, childSQL)
	// TODO: consider if we need to support RIF.

	// Create the test server fixture.
	r.NoError(err)
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	connCtx := stopper.WithContext(timeoutCtx)
	testFixture, cancel, err := newTestFixture(connCtx, serverCfg)
	defer cancel()
	r.NoError(err)

	// This part is mostly figured out, really just need to understand which
	// exact fixutre should be used here.
	// This did the trick really to get the data properly to the target.
	// Though I may have something flipped here and I may not know about it.
	// Really need to think on this logic.
	// For some reason this needs to be the target fixture.
	acc := types.OrderedAcceptorFrom(targetFixture.ApplyAcceptor, targetFixture.Watchers)

	// TODO: setup the changefeed here.
	// TODO: commonize the changefeed setup.
	// Do sourcepool exec and give the whole string for the changefeed
	method, priv, err := jwtAuth.InsertTestingKey(ctx, targetFixture.StagingPool, testFixture.Authenticator, targetFixture.StagingDB)
	r.NoError(err)

	sourceVersion := sourceFixture.SourcePool.Version
	targetDB := targetSchema.Schema()
	target := ident.NewTable(targetDB, targetChecker.Parent.Name().Table())
	_, token, err := jwtAuth.Sign(method, priv, []ident.Schema{target.Schema(), diag.Schema})
	r.NoError(err)

	params := make(url.Values)

	// Set up the changefeed.
	var feedURL url.URL
	var pathIdent ident.Identifier
	createStmt := "CREATE CHANGEFEED"
	if cfg.queries {
		pathIdent = target
	} else {
		pathIdent = target.Schema()
		createStmt += fmt.Sprintf(" FOR TABLE %s, %s", sourceGeneratorWorkload.Parent, sourceGeneratorWorkload.Child)
	}
	if cfg.webhook {
		params.Set("insecure_tls_skip_verify", "true")
		feedURL = url.URL{
			Scheme:   "webhook-https",
			Host:     testFixture.Listener.Addr().String(),
			Path:     ident.Join(pathIdent, ident.Raw, '/'),
			RawQuery: params.Encode(),
		}
		createStmt += " INTO '" + feedURL.String() + "' " +
			" WITH updated," +
			"     resolved='1s'," +
			"     webhook_auth_header='Bearer " + token + "'"
	} else {
		// No webhook_auth_header, so bake it into the query string.
		// See comments in cdc.Handler.ServeHTTP checkAccess.
		params.Set("access_token", token)
		feedURL = url.URL{
			Scheme:   "experimental-http",
			Host:     testFixture.Listener.Addr().String(),
			Path:     ident.Join(pathIdent, ident.Raw, '/'),
			RawQuery: params.Encode(),
		}
		createStmt += " INTO '" + feedURL.String() + "' " +
			"WITH updated,resolved='1s'"
	}
	if cfg.diff {
		createStmt += ",diff"
	}
	// Don't wait the entire 30s. This options was introduced in the
	// same versions as webhooks.
	if ok, err := supportsMinCheckpoint(sourceVersion); err == nil && ok {
		createStmt += ",min_checkpoint_frequency='1s'"
	}
	if cfg.queries {
		createStmt += ",envelope='wrapped',format='json'"
		createStmt += " AS SELECT pk, val"
		createStmt += " FROM %s"
	}

	log.Debugf("changefeed URL is %s", feedURL.String())
	log.Debugf("create stmt is %s", createStmt)
	_, err = targetFixture.SourcePool.ExecContext(ctx, createStmt)
	r.NoError(err)

	for i := range maxIterations {
		batch := &types.MultiBatch{}
		sourceGeneratorWorkload.GenerateInto(batch, hlc.New(int64(i), i+1))

		// Insert data on the source since it will flow from changefeeds
		// to the staging DB and then to the target.
		tx, err := sourceFixture.TargetPool.BeginTx(ctx, &sql.TxOptions{})
		r.NoError(err)
		r.NoError(acc.AcceptMultiBatch(ctx, batch, &types.AcceptOptions{TargetQuerier: tx}))
		r.NoError(tx.Commit())
	}

	// Merge the generators in to the target checker.
	// This makes it so that the target checker has all the expected
	// data from the source generator workload.
	targetChecker.CopyFrom(sourceGeneratorWorkload)

	// Adapted this polling logic from the above test.
	// This is a simpler way to determine if the row
	// was backfilled on the target.
	for {
		ct, err := base.GetRowCount(ctx, targetFixture.TargetPool, target)
		r.NoError(err)
		if ct >= 1 {
			break
		}
		log.Debug("waiting for target rows to be written")
		time.Sleep(time.Second)
	}

	r.True(targetChecker.CheckConsistent(ctx, t))

	// We need to wait for the connection to shut down, otherwise the
	// database cleanup callbacks (to drop the publication, etc.) from
	// the test code above can't succeed.
	connCtx.Stop(time.Minute)
	<-connCtx.Done()

	// This is all debug logging that should be used to get an idea
	// of what's going on with the test.

	/*
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s;", sourceGeneratorWorkload.Parent)
		fmt.Println("source query: ", query)
		rows, err = targetFixture.SourcePool.Query(query)
		require.NoError(t, err)
		for rows.Next() {
			var item string
			err := rows.Scan(&item)
			r.NoError(err)
			fmt.Println("source count: ", item)
		}

		// Target row counts.
		// An important thing to note here is that the target row counts are empty.
		// TODO: figure out why I need to do the target schema joined with the
		// source table names. Why do the table names differ here?
		// Well at least I know it works now.
		query = fmt.Sprintf("SELECT COUNT(*) FROM %s;", ident.NewTable(targetSchema, sourceGeneratorWorkload.Parent.Table()))
		fmt.Println("target query: ", query)
		rows, err = targetFixture.TargetPool.Query(query)
		require.NoError(t, err)
		for rows.Next() {
			var item string
			err := rows.Scan(&item)
			r.NoError(err)
			fmt.Println("target count: ", item)
		}

		// So, this monkey patch works and it's because the source has
		// 7 parents, but then the target has 0, predictably.
		//targetChecker.Parents = sourceGeneratorWorkload.Parents

		fmt.Println("checked that they are consistent")
		fmt.Println("source final: ", targetFixture.SourcePool.ConnectionString)
		fmt.Println("target final: ", targetFixture.TargetPool.ConnectionString)
		fmt.Println("source schema: ", targetFixture.SourceSchema)
		fmt.Println("target schema: ", targetFixture.TargetSchema)

		// So we now know that the source generator workload parent and child rows
		// check out :) . That means we have the relevant data.
		// However the target row counts are empty as expected.
		// So why is this test passing......
		fmt.Println("source row counts: ", sourceGeneratorWorkload.Parent, sourceGeneratorWorkload.ChildRows(), sourceGeneratorWorkload.ParentRows())
		fmt.Println("target row counts: ", targetChecker.Parent, targetChecker.ChildRows(), targetChecker.ParentRows())

		// OK cool, so here are my findings now that I verified that the changefeed
		// is working fine......
		// In order to make sure data ends up in the target, the source and target
		// schemas need to have tables that are the same name. This is just how the
		// webhook operates (based on the path and the message coming in that has
		// table info).
		// Right now, the problem that we have is that the tables from the target
		// checker and the one from the source generator workload are different.
		// This makes it so that when we check the workload on the target, it shows
		// as 0 rows, because even though data is written, it's written to tables 3
		// and 4, not 1 and 2.
		// Right now, I need to figure out how I can fix the the table names so that
		// they are consistent. Maybe I just need to override this by setting
		// target.Parent =, target.Child = ident.NewTable(targetSchema,
		// sourceGeneratorWorkload.Parent.Name().Table()). But this is a bit hacky.
		// Think more on this tomorrow.
	*/
}
