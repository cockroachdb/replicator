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
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/replicator/internal/conveyor"
	"github.com/cockroachdb/replicator/internal/script"
	"github.com/cockroachdb/replicator/internal/sinkprod"
	"github.com/cockroachdb/replicator/internal/sinktest"
	"github.com/cockroachdb/replicator/internal/sinktest/all"
	"github.com/cockroachdb/replicator/internal/sinktest/base"
	"github.com/cockroachdb/replicator/internal/sinktest/scripttest"
	"github.com/cockroachdb/replicator/internal/source/objstore/providers/s3"
	"github.com/cockroachdb/replicator/internal/types"
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
	endpoint = "localhost:9100"
	user     = "root"
	password = "SoupOrSecret"
)

// TestMain verifies that we can run the integration test for objstore.
func TestMain(m *testing.M) {
	all.IntegrationMain(m, all.ObjectStoreName)
}

// TestIntegrationSmoke verifies that we can process change events stored in an object store.
func TestIntegrationSmoke(t *testing.T) {

	t.Run("consistent", func(t *testing.T) { testIntegration(t, &fixtureConfig{}) })
	t.Run("consistent chaos", func(t *testing.T) { testIntegration(t, &fixtureConfig{chaos: true}) })
	t.Run("consistent script", func(t *testing.T) { testIntegration(t, &fixtureConfig{script: true}) })

	t.Run("immediate", func(t *testing.T) { testIntegration(t, &fixtureConfig{immediate: true}) })
	t.Run("immediate chaos", func(t *testing.T) { testIntegration(t, &fixtureConfig{chaos: true, immediate: true}) })
	t.Run("immediate script", func(t *testing.T) { testIntegration(t, &fixtureConfig{script: true, immediate: true}) })
}

func testIntegration(t *testing.T, fc *fixtureConfig) {
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

	cleanup, err := createBucket(ctx, serverCfg.s3, bucketName)
	r.NoError(err)
	defer func() {
		if err := cleanup(); err != nil {
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

func createBucket(ctx context.Context, config *s3.Config, bucketName string) (func() error, error) {
	minioClient, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.AccessKey, config.SecretKey, ""),
		Secure: !config.Insecure,
	})
	if err != nil {
		return nil, err
	}
	cleanup := func() error {
		for item := range minioClient.ListObjects(ctx, bucketName,
			minio.ListObjectsOptions{Recursive: true}) {
			err = minioClient.RemoveObject(ctx, bucketName, item.Key, minio.RemoveObjectOptions{})
			if err != nil {
				return err
			}
		}
		return minioClient.RemoveBucket(ctx, bucketName)
	}
	return cleanup, minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
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
		config.Sequencer.Chaos = 0.0005
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
