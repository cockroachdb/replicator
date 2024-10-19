// Copyright 2024 The Cockroach Authors
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

package objstore

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/replicator/internal/conveyor"
	"github.com/cockroachdb/replicator/internal/script"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/sinkprod"
	"github.com/cockroachdb/replicator/internal/sinktest"
	"github.com/cockroachdb/replicator/internal/sinktest/all"
	"github.com/cockroachdb/replicator/internal/sinktest/base"
	"github.com/cockroachdb/replicator/internal/sinktest/scripttest"
	"github.com/cockroachdb/replicator/internal/source/objstore/providers/s3"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fixtureConfig struct {
	chaos     bool
	script    bool
	immediate bool
}

const (
	endpoint      = "localhost:9100"
	maxBatchSize  = 50
	maxIterations = 100
	password      = "SoupOrSecret"
	uniquer       = "8446dc14536f66f2-1-2-00000000"
	user          = "root"
)

// TestMain verifies that we can run the integration test for objstore.
func TestMain(m *testing.M) {
	all.IntegrationMain(m, all.ObjectStoreName)
}

// TestSmoke verifies that we can process change events sent by a CockroachDB node
// via a bucket.
func TestSmoke(t *testing.T) {

	t.Run("consistent", func(t *testing.T) { testSmoke(t, &fixtureConfig{}) })
	t.Run("consistent chaos", func(t *testing.T) { testSmoke(t, &fixtureConfig{chaos: true}) })
	t.Run("consistent script", func(t *testing.T) { testSmoke(t, &fixtureConfig{script: true}) })

	t.Run("immediate", func(t *testing.T) { testSmoke(t, &fixtureConfig{immediate: true}) })
	t.Run("immediate chaos", func(t *testing.T) { testSmoke(t, &fixtureConfig{chaos: true, immediate: true}) })
	t.Run("immediate script", func(t *testing.T) { testSmoke(t, &fixtureConfig{script: true, immediate: true}) })
}

func testSmoke(t *testing.T, fc *fixtureConfig) {
	a := assert.New(t)
	r := require.New(t)

	// Create a basic fixture to represent a source database.
	sourceFixture, err := base.NewFixture(t)
	r.NoError(err)

	ctx := sourceFixture.Context

	// Create a basic destination database connection.
	destFixture, err := base.NewFixture(t)
	r.NoError(err)

	targetDB := destFixture.TargetSchema.Schema()
	targetPool := destFixture.TargetPool
	bucketName := targetDB.Raw()

	// Set up source and target tables.
	source, err := sourceFixture.CreateSourceTable(ctx, "CREATE TABLE %s (pk INT PRIMARY KEY, v STRING)")
	r.NoError(err)

	// Since we're creating the target table without using the helper
	// CreateTable(), we need to manually refresh the target's Watcher.
	target := ident.NewTable(targetDB, source.Name().Table())
	targetCol := "v"
	if fc.script {
		targetCol = "v_mapped"
	}
	_, err = targetPool.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (pk INT PRIMARY KEY, %s VARCHAR(2048))", target, targetCol))
	r.NoError(err)

	serverCfg, err := getConfig(destFixture, fc, target, bucketName)
	r.NoError(err)
	if fc.immediate {
		r.Equal(1, serverCfg.Workers)
	}
	sourceBucket, err := newSourceBucket(ctx, serverCfg.s3)
	r.NoError(err)
	defer func() {
		if err := sourceBucket.cleanup(ctx); err != nil {
			log.Errorf("error removing bucket %v", err)
		}
	}()
	conn, err := Start(ctx, serverCfg)
	r.NoError(err)

	conveyor := conn.Conn.conveyor
	watcher := conveyor.(interface{ Watcher() types.Watcher }).Watcher()

	r.NoError(watcher.Refresh(ctx, targetPool))

	// We will be using the target db as the bucket name.
	createStmt := "CREATE CHANGEFEED FOR TABLE %s INTO '" +
		storageURL(bucketName) +
		"' WITH updated,diff,resolved='5s',min_checkpoint_frequency='5s'"
	r.NoError(source.Exec(ctx, createStmt))

	// Add base data to the source table.
	r.NoError(source.Exec(ctx, "INSERT INTO %s (pk, v) VALUES (1, 'one')"))
	ct, err := source.RowCount(ctx)
	r.NoError(err)
	a.Equal(1, ct)

	// Wait for the backfilled value.
	for {
		ct, err := base.GetRowCount(ctx, targetPool, target)
		r.NoError(err)
		if ct >= 1 {
			break
		}
		log.Infof("waiting for backfill %s", target)
		time.Sleep(time.Second)
	}

	// Update the first value
	r.NoError(source.Exec(ctx, "UPSERT INTO %s (pk, v) VALUES (1, 'updated')"))

	// Insert an additional value
	r.NoError(source.Exec(ctx, "INSERT INTO %s (pk, v) VALUES (2, 'two')"))
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
		log.Infof("waiting for stream %s", target)
		time.Sleep(time.Second)
	}

	// Also wait to see that the update was applied.
	for {
		var val string
		r.NoError(targetPool.QueryRowContext(ctx,
			fmt.Sprintf("SELECT %s FROM %s WHERE pk = 1", targetCol, target),
		).Scan(&val))
		if val == "updated" {
			break
		}
		log.Debug("waiting for update")
		time.Sleep(100 * time.Second)
	}
	metrics, err := prometheus.DefaultGatherer.Gather()
	a.NoError(err)
	log.WithField("metrics", metrics).Trace()
	sinktest.CheckDiagnostics(ctx, t, conn.Diagnostics)
}

func TestWorkload(t *testing.T) {
	t.Run("consistent", func(t *testing.T) { testWorkload(t, &fixtureConfig{}) })
	t.Run("consistent chaos", func(t *testing.T) { testWorkload(t, &fixtureConfig{chaos: true}) })

	t.Run("immediate", func(t *testing.T) { testWorkload(t, &fixtureConfig{immediate: true}) })
	t.Run("immediate chaos", func(t *testing.T) { testWorkload(t, &fixtureConfig{chaos: true, immediate: true}) })
}

func testWorkload(t *testing.T, fc *fixtureConfig) {
	r := require.New(t)

	fixture, err := all.NewFixture(t, time.Minute)
	r.NoError(err)
	ctx := fixture.Context
	workload, _, err := fixture.NewWorkload(ctx,
		&all.WorkloadConfig{
			// Don't create foreign keys references in immediate mode
			DisableFK:      fc.immediate,
			DisableStaging: true,
		})
	r.NoError(err)
	bucketName := fixture.TargetSchema.Schema().Raw()
	serverCfg, err := getConfig(fixture.Fixture, fc,
		workload.Parent.Name(), bucketName)
	r.NoError(err)
	// With immediate mode, we need to process files sequentially.
	if fc.immediate {
		r.Equal(1, serverCfg.Workers)
	}
	// Using a flat partition format in this test.
	serverCfg.PartitionFormat = Flat

	sourceBucket, err := newSourceBucket(ctx, serverCfg.s3)
	r.NoError(err)
	defer func() {
		if err := sourceBucket.cleanup(ctx); err != nil {
			log.Errorf("error removing bucket %v", err)
		}
	}()

	conn, err := Start(ctx, serverCfg)
	r.NoError(err)
	stats := conn.Conn.conveyor.(interface {
		Stat() *notify.Var[sequencer.Stat]
	}).Stat()

	var clock hlc.Clock
	r.NoError(sourceBucket.writeResolved(ctx, clock.Now()))
	for iter := 1; iter <= maxIterations; iter++ {
		batch := &types.MultiBatch{}
		r.NoError(err)
		size := rand.IntN(maxBatchSize) + 1
		for i := 0; i < size; i++ {
			workload.GenerateInto(batch, clock.Now())
		}
		r.NoError(sourceBucket.writeBatch(ctx, batch))
		// Write a resolved timestamp file once in a while.
		// Ensure that we have resolved file at the end.
		if isPrime(iter) || iter == maxIterations {
			r.NoError(sourceBucket.writeResolved(ctx, clock.Now()))
		}
	}
	// Waiting for the rows to show in the target database.
	r.NoError(workload.WaitForCatchUp(ctx, stats))

	parent, err := workload.Checker.StageCounter(workload.Parent.Name(),
		hlc.RangeIncluding(hlc.Zero(), clock.Last()))
	r.NoError(err)
	child, err := workload.Checker.StageCounter(workload.Child.Name(),
		hlc.RangeIncluding(hlc.Zero(), clock.Last()))
	r.NoError(err)
	log.Infof("staging database content parent rows: %d, child rows: %d", parent, child)

	// Verify that the target database has all the data.
	workload.CheckConsistent(ctx, t)
}

type sourceBucket struct {
	client     *minio.Client
	bucketName string
}

func newSourceBucket(ctx context.Context, config *s3.Config) (*sourceBucket, error) {
	client, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.AccessKey, config.SecretKey, ""),
		Secure: !config.Insecure,
	})
	if err != nil {
		return nil, err
	}
	if err := client.MakeBucket(ctx, config.Bucket, minio.MakeBucketOptions{}); err != nil {
		return nil, err
	}
	return &sourceBucket{
		client:     client,
		bucketName: config.Bucket,
	}, nil
}

func (b *sourceBucket) cleanup(ctx context.Context) error {
	for item := range b.client.ListObjects(ctx, b.bucketName,
		minio.ListObjectsOptions{Recursive: true}) {
		if err := b.client.RemoveObject(ctx, b.bucketName,
			item.Key, minio.RemoveObjectOptions{}); err != nil {
			return err
		}
	}
	return b.client.RemoveBucket(ctx, b.bucketName)
}

func (b *sourceBucket) writeBatch(ctx context.Context, batch *types.MultiBatch) error {
	type payload struct {
		After   json.RawMessage `json:"after"`
		Before  json.RawMessage `json:"before"`
		Key     json.RawMessage `json:"key"`
		Updated string          `json:"updated"`
	}
	var buffers ident.TableMap[*strings.Builder]
	var timestamps ident.TableMap[hlc.Time]
	for _, b := range batch.Data {
		for t := range b.Data.Values() {
			for _, v := range t.Data {
				p := &payload{
					After:   v.Data,
					Before:  v.Before,
					Key:     v.Key,
					Updated: v.Time.String(),
				}
				// Changefeed encodes a deletion as an absence of an
				// after block.
				if v.IsDelete() {
					p.After = nil
				}
				out, err := json.Marshal(p)
				if err != nil {
					return err
				}
				buff, ok := buffers.Get(t.Table)
				if !ok {
					buff = &strings.Builder{}
					buffers.Put(t.Table, buff)
					timestamps.Put(t.Table, t.Time)
				}
				if _, err = buff.Write(out); err != nil {
					return err
				}
				if _, err = buff.WriteString("\n"); err != nil {
					return err
				}
			}
		}
	}
	for k, v := range buffers.All() {
		timestamp, _ := timestamps.Get(k)
		tm := time.Unix(0, timestamp.Nanos())
		date := tm.Format("20060102150405")
		objectName := fmt.Sprintf(`%s%09d%010d-%s-%s-%d.ndjson`,
			date,
			tm.Nanosecond(), timestamp.Logical(),
			uniquer,
			k.Table().Raw(), 1,
		)
		if _, err := b.client.PutObject(ctx, b.bucketName, objectName,
			strings.NewReader(v.String()), int64(v.Len()),
			minio.PutObjectOptions{},
		); err != nil {
			return err
		}
	}
	return nil
}

func (b *sourceBucket) writeResolved(ctx context.Context, resolved hlc.Time) error {
	type resolvedPayload struct {
		Resolved string `json:"resolved"`
	}
	var buff strings.Builder
	r := resolvedPayload{
		Resolved: resolved.String(),
	}
	out, err := json.Marshal(r)
	if err != nil {
		return err
	}
	buff.Write(out)
	buff.WriteString("\n")
	tm := time.Unix(0, resolved.Nanos())
	date := tm.Format("20060102150405")
	objectName := fmt.Sprintf(`%s%09d%010d.RESOLVED`,
		date,
		tm.Nanosecond(), resolved.Logical())
	_, err = b.client.PutObject(ctx, b.bucketName, objectName,
		strings.NewReader(buff.String()), int64(buff.Len()),
		minio.PutObjectOptions{},
	)
	return err

}

// getConfig is a helper function to create a configuration for the connector
func getConfig(
	fixture *base.Fixture, fc *fixtureConfig, tgt ident.Table, bucketName string,
) (*Config, error) {
	dbName := fixture.TargetSchema.Schema()
	crdbPool := fixture.TargetPool
	config := &Config{
		Conveyor: conveyor.Config{
			Immediate: fc.immediate,
		},
		Staging: sinkprod.StagingConfig{
			Schema: fixture.StagingDB.Schema(),
		},
		Target: sinkprod.TargetConfig{
			CommonConfig: sinkprod.CommonConfig{
				Conn: crdbPool.ConnectionString,
			},
			ApplyTimeout: 2 * time.Minute, // Increase to make using the debugger easier.
		},
		// Connector specific parameters
		BufferSize:   defaultBufferSize,
		FetchDelay:   defaultFetchDelay,
		StorageURL:   storageURL(bucketName),
		TargetSchema: dbName,
		Workers:      defaultNumberOfWorkers,
	}
	if fc.chaos {
		config.Sequencer.Chaos = 2
	}
	if fc.script {
		config.Script = script.Config{
			FS:       scripttest.ScriptFSFor(tgt),
			MainPath: "/testdata/logical_test.ts",
		}
	}
	return config, config.Preflight(fixture.Context)
}

func storageURL(bucket string) string {
	return fmt.Sprintf("s3://%s?AWS_ACCESS_KEY_ID=%s&AWS_SECRET_ACCESS_KEY=%s&AWS_ENDPOINT=http://%s",
		bucket, user, password, endpoint)
}
