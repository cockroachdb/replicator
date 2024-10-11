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

// ChangefeedConfig is meant to influence the behavior
// of the created changefeed statement depending
// on the options defined. This can be extended later on
// to handle various configurations and webhook parameters.
type ChangefeedConfig struct {
	diff    bool
	queries bool
	webhook bool
}

// CreateChangefeed creates a changefeed on the source CRDB side that is
// compatible with the replicator webhook. It is an intentional decision to make
// the input parameters as specific as possible so that the caller doesn't need
// to construct whole structs with unrelated information to create the
// changefeed. This makes it so this is more portable for future work that needs
// this: for example, integrating the e2e workload checker with this logic.
func CreateChangefeedStatement(
	cfg *ChangefeedConfig,
	host string,
	target ident.Table,
	token string,
	tables []ident.Table,
	sourceVersion string,
) string {
	params := make(url.Values)
	var feedURL url.URL
	var pathIdent ident.Identifier
	createStmt := "CREATE CHANGEFEED"
	if cfg.queries {
		pathIdent = target
	} else {
		// Creating the comma separated table string
		// that the changefeed requires.
		tablesStr := ""
		for i, table := range tables {
			if i > 0 {
				tablesStr += ", "
			}
			tablesStr += table.String()
		}
		pathIdent = target.Schema()
		createStmt += fmt.Sprintf(" FOR TABLE %s", tablesStr)
	}
	if cfg.webhook {
		params.Set("insecure_tls_skip_verify", "true")
		feedURL = url.URL{
			Scheme:   "webhook-https",
			Host:     host,
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
			Host:     host,
			Path:     ident.Join(pathIdent, ident.Raw, '/'),
			RawQuery: params.Encode(),
		}
		createStmt += " INTO '" + feedURL.String() + "' " +
			"WITH updated, resolved='1s'"
	}
	if cfg.diff {
		createStmt += ", diff"
	}
	// Don't wait the entire 30s. This option was introduced in the
	// same versions as webhooks.
	if ok, err := supportsMinCheckpoint(sourceVersion); err == nil && ok {
		createStmt += ", min_checkpoint_frequency='1s'"
	}

	if cfg.queries {
		// CDC queries only support a single table at a time.
		// Safe to just use the first table passed in.
		createStmt += ",envelope='wrapped',format='json'"
		createStmt += " AS SELECT pk, val"
		createStmt += fmt.Sprintf(" FROM %s", tables[0].String())
	}
	log.Debugf("changefeed URL is %s", feedURL.String())

	return createStmt
}

func TestCreateChangefeedStatement(t *testing.T) {
	type args struct {
		cfg           *ChangefeedConfig
		host          string
		target        ident.Table
		token         string
		tables        []ident.Table
		sourceVersion string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "basic no changefeed configs",
			args: args{
				cfg:           &ChangefeedConfig{},
				host:          "localhost:8080",
				target:        ident.NewTable(ident.MustSchema(ident.New("target"), ident.New("public")), ident.New("tbl1")),
				token:         "my_token",
				tables:        []ident.Table{ident.NewTable(ident.MustSchema(ident.New("source"), ident.New("public")), ident.New("tbl1"))},
				sourceVersion: "CockroachDB CCL v24.2.1 ",
			},
			want: `CREATE CHANGEFEED FOR TABLE "source"."public"."tbl1" INTO 'experimental-http://localhost:8080/target/public?access_token=my_token' WITH updated, resolved='1s', min_checkpoint_frequency='1s'`,
		},
		{
			name: "basic webhook",
			args: args{
				cfg: &ChangefeedConfig{
					webhook: true,
				},
				host:   "localhost:8080",
				target: ident.NewTable(ident.MustSchema(ident.New("target"), ident.New("public")), ident.New("tbl1")),
				token:  "my_token",
				tables: []ident.Table{
					ident.NewTable(ident.MustSchema(ident.New("source"), ident.New("public")), ident.New("tbl1")),
					ident.NewTable(ident.MustSchema(ident.New("source"), ident.New("public")), ident.New("tbl2")),
				},
				sourceVersion: "CockroachDB CCL v24.2.1 ",
			},
			want: `CREATE CHANGEFEED FOR TABLE "source"."public"."tbl1", "source"."public"."tbl2" INTO 'webhook-https://localhost:8080/target/public?insecure_tls_skip_verify=true'  WITH updated,     resolved='1s',     webhook_auth_header='Bearer my_token', min_checkpoint_frequency='1s'`,
		},
		{
			name: "webhook and diff enabled",
			args: args{
				cfg: &ChangefeedConfig{
					webhook: true,
					diff:    true,
				},
				host:   "localhost:8080",
				target: ident.NewTable(ident.MustSchema(ident.New("target"), ident.New("public")), ident.New("tbl1")),
				token:  "my_token",
				tables: []ident.Table{
					ident.NewTable(ident.MustSchema(ident.New("source"), ident.New("public")), ident.New("tbl1")),
					ident.NewTable(ident.MustSchema(ident.New("source"), ident.New("public")), ident.New("tbl2")),
				},
				sourceVersion: "CockroachDB CCL v24.2.1 ",
			},
			want: `CREATE CHANGEFEED FOR TABLE "source"."public"."tbl1", "source"."public"."tbl2" INTO 'webhook-https://localhost:8080/target/public?insecure_tls_skip_verify=true'  WITH updated,     resolved='1s',     webhook_auth_header='Bearer my_token', diff, min_checkpoint_frequency='1s'`,
		},
		{
			name: "basic webhook CDC queries",
			args: args{
				cfg: &ChangefeedConfig{
					webhook: true,
					queries: true,
				},
				host:   "localhost:8080",
				target: ident.NewTable(ident.MustSchema(ident.New("target"), ident.New("public")), ident.New("tbl1")),
				token:  "my_token",
				tables: []ident.Table{
					ident.NewTable(ident.MustSchema(ident.New("source"), ident.New("public")), ident.New("tbl1")),
				},
				sourceVersion: "CockroachDB CCL v24.2.1 ",
			},
			want: `CREATE CHANGEFEED INTO 'webhook-https://localhost:8080/target/public/tbl1?insecure_tls_skip_verify=true'  WITH updated,     resolved='1s',     webhook_auth_header='Bearer my_token', min_checkpoint_frequency='1s',envelope='wrapped',format='json' AS SELECT pk, val FROM "source"."public"."tbl1"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CreateChangefeedStatement(tt.args.cfg, tt.args.host, tt.args.target, tt.args.token, tt.args.tables, tt.args.sourceVersion)
			require.Equal(t, tt.want, got)
		})
	}
}

const maxIterations = 25

func TestWorkload(t *testing.T) {
	testWorkload(t)
}

func testWorkload(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	r := require.New(t)

	// Create the target and source fixtures, which will be used
	// later on to generate data into the source and check that
	// target rows are created properly.
	targetFixture, err := all.NewFixture(t)
	r.NoError(err)

	sourceFixture, err := all.NewFixtureFromBase(targetFixture.Swapped())
	r.NoError(err)

	cfg := &testConfig{webhook: true}
	// The original source is from the target fixture.
	sourceVersion := targetFixture.SourcePool.Version
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

	ctx := targetFixture.Context
	targetChecker, _, err := targetFixture.NewWorkload(ctx, &all.WorkloadConfig{})
	r.NoError(err)

	sourceSchema := targetFixture.SourceSchema.Schema()
	targetSchema := targetFixture.TargetSchema.Schema()

	parent := ident.NewTable(sourceSchema, targetChecker.Parent.Name().Table())
	child := ident.NewTable(sourceSchema, targetChecker.Child.Name().Table())
	sourceGeneratorWorkload := workload.NewGeneratorBase(parent, child)
	r.NoError(err)

	// Creates the tables on the source side, so that the same table
	// names exist n both source and target, a requirement for
	// replication here.
	sourcePool := targetFixture.SourcePool
	parent = sourceGeneratorWorkload.Parent
	child = sourceGeneratorWorkload.Child
	parentSQL, childSQL := all.WorkloadSchema(
		&all.WorkloadConfig{}, types.ProductPostgreSQL,
		parent, child)
	_, err = sourcePool.ExecContext(ctx, parentSQL)
	r.NoError(err)
	_, err = sourcePool.ExecContext(ctx, childSQL)
	r.NoError(err)

	// Setup test configurations.
	serverCfg, err := getConfig(cfg, sourceFixture, targetFixture.TargetPool)
	r.NoError(err)

	// Create the test server fixture.
	// Preflight sets default values that are not set in the testConfig.
	r.NoError(serverCfg.Preflight())

	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	connCtx := stopper.WithContext(timeoutCtx)
	testFixture, cancel, err := newTestFixture(connCtx, serverCfg)
	defer cancel()
	r.NoError(err)

	// Insert a testing key so we can properly talk to the webhook
	// in an authenticated manner.
	method, priv, err := jwtAuth.InsertTestingKey(ctx, targetFixture.StagingPool, testFixture.Authenticator, targetFixture.StagingDB)
	r.NoError(err)

	targetDB := targetSchema.Schema()
	target := ident.NewTable(targetDB, targetChecker.Parent.Name().Table())
	_, token, err := jwtAuth.Sign(method, priv, []ident.Schema{target.Schema(), diag.Schema})
	r.NoError(err)

	// Create the changefeed on the source CRDB.
	tables := []ident.Table{sourceGeneratorWorkload.Parent, sourceGeneratorWorkload.Child}
	host := testFixture.Listener.Addr().String()
	createStmt := CreateChangefeedStatement(&ChangefeedConfig{
		diff:    cfg.diff,
		queries: cfg.queries,
		webhook: cfg.webhook,
	},
		host,
		target,
		token,
		tables,
		sourceVersion)
	r.NoError(err)
	log.Debugf("create changefeed statement is %s", createStmt)
	_, err = sourcePool.ExecContext(ctx, createStmt)
	r.NoError(err)

	// Make this the target fixture for the accumulator. This is
	// required for the data to write properly later on when
	// we accumulate the batch.
	acc := types.OrderedAcceptorFrom(targetFixture.ApplyAcceptor, targetFixture.Watchers)

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

	// Merge the generator values into the target checker.
	// This makes it so that the target checker has all the expected
	// data from the source generator workload.
	targetChecker.CopyFrom(sourceGeneratorWorkload)

	// Adapted this polling logic from the above test.
	// This is a simpler way to determine if the rows
	// were backfilled on the target.
	for {
		ct, err := base.GetRowCount(ctx, targetFixture.TargetPool, target)
		r.NoError(err)
		// We know we generate 7 items here in the parent.
		if ct >= 7 {
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
}
