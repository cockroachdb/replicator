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

// To execute these tests, set the following environment variables:
//   CDC_INTEGRATION=firestore
//   FIRESTORE_EMULATOR_HOST=127.0.0.1:8181

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/target/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/util/batches"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	enableWipe = true
	sinktest.IntegrationMain(m, sinktest.FirestoreName)
}

func TestSmoke(t *testing.T) {
	t.Run("normal", func(t *testing.T) { testSmoke(t, 0) })
	t.Run("chaos", func(t *testing.T) { testSmoke(t, 0.001) })
}

func testSmoke(t *testing.T, chaosProb float32) {
	a := assert.New(t)
	r := require.New(t)
	const docCount = 200

	// Create a target database.
	fixture, cancel, err := sinktest.NewFixture()
	r.NoError(err)
	defer cancel()

	ctx := fixture.Context
	// Mangle our DB ident into something the emulator will accept.
	projectID := strings.ReplaceAll(fixture.TestDB.Ident().Raw(), "_", "")

	// Create the target schema.
	destTable, err := fixture.CreateTable(ctx,
		"CREATE TABLE %s (id STRING PRIMARY KEY, v STRING, updated_at TIMESTAMP)")
	r.NoError(err)

	// Create a child table, to test the case where an update from a
	// collection gets written to multiple tables with FK relationships.
	childTable, err := fixture.CreateTable(ctx, fmt.Sprintf(
		"CREATE TABLE %%s (id STRING PRIMARY KEY REFERENCES %s ON DELETE CASCADE)",
		destTable))
	r.NoError(err)

	// Create a table for a collection-group to be synced.
	subTable, err := fixture.CreateTable(ctx,
		"CREATE TABLE %s (id STRING PRIMARY KEY, v STRING, updated_at TIMESTAMP)")
	r.NoError(err)

	now := time.Now().UTC()

	// Create a connection to the emulator, to populate source docs. The
	// source docs are created with timestamps that are well in the
	// past.
	fs, err := firestore.NewClient(ctx, projectID)
	r.NoError(err)
	coll := fs.Collection(destTable.Name().Table().Raw())
	docRefs := make([]*firestore.DocumentRef, docCount)
	dynRefs := make([]*firestore.DocumentRef, docCount)
	subRefs := make([]*firestore.DocumentRef, docCount)
	for i := range docRefs {
		docRefs[i], _, err = coll.Add(ctx, map[string]any{
			"v":          fmt.Sprintf("value %d", i),
			"updated_at": now.Add(-time.Hour + time.Duration(i)*time.Second),
		})
		r.NoError(err)
		log.Tracef("inserted %s", docRefs[i].Path)

		// Add sub-collection, to test collection-group queries.
		subRefs[i], _, err = docRefs[i].Collection("subcollection").
			Add(ctx, map[string]any{
				"v":          fmt.Sprintf("value %d", i),
				"updated_at": now.Add(-time.Hour + time.Duration(i)*time.Second),
			})
		r.NoError(err)

		// Add a dynamic sub-collection, to test recursion.
		dynRefs[i], _, err = subRefs[i].Collection(fmt.Sprintf("dynamic_%d", i)).
			Add(ctx, map[string]any{
				"v":          fmt.Sprintf("value %d", i),
				"updated_at": now.Add(-time.Hour + time.Duration(i)*time.Second),
			})
		r.NoError(err)
	}

	x := map[string]struct{}{}
	for _, doc := range docRefs {
		if _, found := x[doc.ID]; found {
			runtime.Breakpoint()
		}
		x[doc.ID] = struct{}{}
	}
	for _, doc := range subRefs {
		if _, found := x[doc.ID]; found {
			runtime.Breakpoint()
		}
		x[doc.ID] = struct{}{}
	}
	for _, doc := range dynRefs {
		if _, found := x[doc.ID]; found {
			runtime.Breakpoint()
		}
		x[doc.ID] = struct{}{}
	}

	cfg := &Config{
		BaseConfig: logical.BaseConfig{
			ApplyTimeout:       2 * time.Minute, // Increase to make using the debugger easier.
			BackfillWindow:     time.Minute,
			ChaosProb:          chaosProb,
			ForeignKeysEnabled: true,
			Immediate:          false,
			LoopName:           "fslogicaltest",
			RetryDelay:         time.Nanosecond,
			StandbyTimeout:     10 * time.Millisecond,
			StagingDB:          fixture.StagingDB.Ident(),
			TargetConn:         fixture.Pool.Config().ConnString(),
			TargetDB:           fixture.TestDB.Ident(),

			ScriptConfig: script.Config{
				MainPath: "/main.ts",
				FS: &fstest.MapFS{
					"main.ts": &fstest.MapFile{
						Data: []byte(fmt.Sprintf(`
import * as api from "cdc-sink@v1";
api.configureSource(%[1]s, {
  deletesTo: %[1]s,            // Use ON DELETE CASCADE
  dispatch: (doc, meta) => ({
    %[1]s: [ doc ],            // Pass through to main table
    %[3]s: [ { id: doc.id } ], // Generate an entry in an FK-referring table.
  })
});
api.configureSource("group:subcollection", { recurse:true, target: %[2]s } );
`, destTable.Name().Table(), subTable.Name().Table(), childTable.Name().Table())),
					},
				},
			},
		},
		BackfillBatchSize:           10,
		DocumentIDProperty:          ident.New("id"), // Map doc id metadata to target column.
		Idempotent:                  true,
		ProjectID:                   projectID,
		TombstoneCollection:         "Tombstones",
		TombstoneCollectionProperty: ident.New("collection"),
		TombstoneIgnoreUnmapped:     true,
		UpdatedAtProperty:           ident.New("updated_at"),
	}

	loops, cancel, err := startLoopsFromFixture(fixture, cfg)
	r.NoError(err)
	defer cancel()
	a.Len(loops, 2)

	log.Info("waiting for top-level backfill")
	for {
		ct, err := destTable.RowCount(ctx)
		r.NoError(err)
		if ct == docCount {
			break
		}
		log.Infof("saw only %d documents in top level", ct)
		time.Sleep(10 * time.Millisecond)
	}

	log.Info("waiting for child-table backfill")
	for {
		ct, err := childTable.RowCount(ctx)
		r.NoError(err)
		if ct == docCount {
			break
		}
		log.Infof("saw only %d documents in child table", ct)
		time.Sleep(10 * time.Millisecond)
	}

	log.Info("waiting for collection-group backfill")
	for {
		ct, err := subTable.RowCount(ctx)
		r.NoError(err)
		// Two times, since we also drop the dynamic sub-collection
		// elements into the same destination table.
		if ct == 2*docCount {
			break
		}
		log.Infof("saw only %d documents in sub-collection", ct)
		time.Sleep(10 * time.Millisecond)
	}

	log.Info("backfill done, sending document updates")

	// Update previous documents in batches. The FS API limits
	// the maximum transaction batch size.
	const fsBatchSize = 100
	r.NoError(batches.Window(fsBatchSize, len(docRefs), func(start, end int) error {
		return fs.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
			for i, docRef := range docRefs[start:end] {
				if err := tx.Set(docRef, map[string]any{
					"v":          fmt.Sprintf("updated %d", i),
					"updated_at": firestore.ServerTimestamp,
				}, firestore.MergeAll); err != nil {
					return err
				}
			}
			for i, subRef := range subRefs[start:end] {
				if err := tx.Set(subRef, map[string]any{
					"v":          fmt.Sprintf("updated %d", i),
					"updated_at": firestore.ServerTimestamp,
				}, firestore.MergeAll); err != nil {
					return err
				}
			}
			return nil
		})
	}))

	log.Info("waiting to receive document updates")

	// Wait for updated values.
	for {
		var ct int
		r.NoError(fixture.Pool.QueryRow(ctx,
			fmt.Sprintf("SELECT count(*) FROM %s WHERE v LIKE 'updated%%'",
				destTable.Name())).Scan(&ct))
		if ct == docCount {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	log.Info("saw updates, writing tombstones")

	tombstones := fs.Collection("Tombstones")

	// Write an unmappable tombstone document to verify ignore-unknown behavior.
	_, _, err = tombstones.Add(ctx, map[string]any{
		"collection": "does-not-exist",
		"id":         "xyzzy",
		"updated_at": firestore.ServerTimestamp,
	})
	r.NoError(err)

	// Write tombstone documents to simulate out-of-band deletion.
	r.NoError(batches.Window(fsBatchSize, len(docRefs), func(start, end int) error {
		return fs.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
			for _, docRef := range docRefs[start:end] {
				if err := tx.Create(
					// The tombstone docid is arbitrary.
					tombstones.Doc("any"+docRef.ID),
					map[string]any{
						"collection": docRef.Parent.ID,
						"id":         docRef.ID,
						"updated_at": firestore.ServerTimestamp,
					}); err != nil {
					return err
				}
			}
			return nil
		})
	}))

	log.Info("wrote tombstones, waiting for deletes")

	// Wait for documents to be deleted.
	for {
		var ct int
		r.NoError(fixture.Pool.QueryRow(ctx,
			fmt.Sprintf("SELECT count(*) FROM %s WHERE v LIKE 'updated%%'",
				destTable.Name())).Scan(&ct))
		if ct == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	log.Info("all deletes done")
}
