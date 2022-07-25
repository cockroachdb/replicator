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
	log.SetLevel(log.TraceLevel)
	a := assert.New(t)

	// Create a target database.
	fixture, cancel, err := sinktest.NewBaseFixture()
	if !a.NoError(err) {
		return
	}
	defer cancel()

	ctx := fixture.Context
	// Mangle our DB ident into something the emulator will accept.
	projectID := strings.ReplaceAll(fixture.TestDB.Ident().Raw(), "_", "-")[1:]

	// Create the target schema.
	destTable, err := fixture.CreateTable(ctx,
		"CREATE TABLE %s (pk INT PRIMARY KEY, v STRING, updated_at TIMESTAMP)")
	if !a.NoError(err) {
		return
	}

	type Doc struct {
		PK        int       `json:"pk"`
		Val       string    `json:"v"`
		UpdatedAt time.Time `json:"updated_at"`
	}
	now := time.Now().UTC()

	// Create a connection to the emulator, to populate source docs.
	fs, err := firestore.NewClient(ctx, projectID)
	if !a.NoError(err) {
		return
	}
	coll := fs.Collection(destTable.Name().Raw())
	for i := 0; i < 100; i++ {
		doc, _, err := coll.Add(ctx, Doc{
			PK:        i + 1,
			Val:       fmt.Sprintf("value %d", i),
			UpdatedAt: now,
		})
		if !a.NoError(err) {
			return
		}
		log.Tracef("inserted %s", doc.Path)
	}
	time.Sleep(10 * time.Second)
	loops, cancel, err := Start(ctx, &Config{
		Config: logical.Config{
			ApplyTimeout:  2 * time.Minute, // Increase to make using the debugger easier.
			AllowBackfill: true,
			ChaosProb:     0,
			Immediate:     true,
			RetryDelay:    time.Nanosecond,
			StagingDB:     fixture.StagingDB.Ident(),
			TargetConn:    fixture.Pool.Config().ConnString(),
			TargetDB:      fixture.TestDB.Ident(),
		},
		ProjectID:         projectID,
		SourceCollections: []string{coll.ID},
		TargetTables:      []ident.Table{destTable.Name()},
		UpdatedAtProperty: ident.New("updated_at"),
	})
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
		if ct == 100 {
			break
		}
	}
}
