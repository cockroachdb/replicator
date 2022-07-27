// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package fslogical

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/target/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	sinktest.IntegrationMain(m, sinktest.FirestoreName)
}

func TestSmoke(t *testing.T) {
	t.Run("normal", func(t *testing.T) { testSmoke(t, 0) })
	t.Run("chaos", func(t *testing.T) { testSmoke(t, 0.005) })
}

func testSmoke(t *testing.T, chaosProb float32) {
	a := assert.New(t)
	const docCount = 1000

	// Create a target database.
	fixture, cancel, err := sinktest.NewFixture()
	if !a.NoError(err) {
		return
	}
	defer cancel()

	ctx := fixture.Context
	// Mangle our DB ident into something the emulator will accept.
	projectID := strings.ReplaceAll(fixture.TestDB.Ident().Raw(), "_", "")

	// Create the target schema.
	destTable, err := fixture.CreateTable(ctx,
		"CREATE TABLE %s (id STRING PRIMARY KEY, v STRING, updated_at TIMESTAMP)")
	if !a.NoError(err) {
		return
	}

	now := time.Now().UTC()

	// Create a connection to the emulator, to populate source docs.
	fs, err := firestore.NewClient(ctx, projectID)
	if !a.NoError(err) {
		return
	}
	coll := fs.Collection(destTable.Name().Raw())
	docIds := make([]string, docCount)
	for i := range docIds {
		doc, _, err := coll.Add(ctx, map[string]interface{}{
			"v":          fmt.Sprintf("value %d", i),
			"updated_at": now.Add(-time.Hour + time.Duration(i)*time.Second),
		})
		if !a.NoError(err) {
			return
		}
		log.Tracef("inserted %s", doc.Path)
		docIds[i] = doc.ID
	}

	cfg := &Config{
		Config: logical.Config{
			ApplyTimeout:   2 * time.Minute, // Increase to make using the debugger easier.
			AllowBackfill:  true,
			ChaosProb:      chaosProb,
			Immediate:      true,
			RetryDelay:     time.Nanosecond,
			StandbyTimeout: 10 * time.Millisecond,
			StagingDB:      fixture.StagingDB.Ident(),
			TargetConn:     fixture.Pool.Config().ConnString(),
			TargetDB:       fixture.TestDB.Ident(),
		},
		BackfillBatchSize:  10,
		DocumentIDProperty: "id", // Map doc id metadata to target column.
		ProjectID:          projectID,
		SourceCollections:  []string{coll.ID},
		TargetTables:       []ident.Table{destTable.Name()},
		UpdatedAtProperty:  ident.New("updated_at"),
	}
	loops, cancel, err := startLoopsFromFixture(fixture, cfg)
	if !a.NoError(err) {
		return
	}
	defer cancel()
	a.Len(loops, 1)

	// Wait for backfill.
	for {
		ct, err := destTable.RowCount(ctx)
		if !a.NoError(err) {
			return
		}
		if ct == docCount {
			break
		}
	}

	// Update previous documents.
	for i, docID := range docIds {
		_, err := coll.Doc(docID).Set(ctx, map[string]interface{}{
			"v":          fmt.Sprintf("updated %d", i),
			"updated_at": firestore.ServerTimestamp,
		})
		if !a.NoError(err) {
			return
		}
		log.Tracef("updated %s", docID)
	}

	// Wait for updated values.
	for {
		var ct int
		if !a.NoError(fixture.Pool.QueryRow(ctx,
			fmt.Sprintf("SELECT count(*) FROM %s WHERE v LIKE 'updated%%'",
				destTable.Name())).Scan(&ct)) {
			return
		}
		if ct == docCount {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
}
