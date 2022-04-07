// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package fan

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/target/apply"
	"github.com/cockroachdb/cdc-sink/internal/target/schemawatch"
	"github.com/cockroachdb/cdc-sink/internal/target/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	"github.com/stretchr/testify/assert"
)

type intStamp int

func (s intStamp) Less(other stamp.Stamp) bool {
	return s < other.(intStamp)
}

func TestFanSmoke(t *testing.T) {
	a := assert.New(t)

	ctx, dbInfo, cancel := sinktest.Context()
	defer cancel()

	dbName, cancel, err := sinktest.CreateDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	watchers, cancel := schemawatch.NewWatchers(dbInfo.Pool())
	defer cancel()

	tbl, err := sinktest.CreateTable(ctx, dbName,
		"CREATE TABLE %s (k INT PRIMARY KEY, v STRING)")
	if !a.NoError(err) {
		return
	}

	appliers, cancel := apply.NewAppliers(watchers)
	defer cancel()

	// Provide a correct way for the callback to report where the
	// consistent point has advanced to.
	consistentUpdated := sync.NewCond(&sync.Mutex{})
	var consistentPoint intStamp

	imm, cancel, err := New(
		appliers,
		2*time.Minute,
		dbInfo.Pool(),
		func(stamp stamp.Stamp) {
			consistentUpdated.L.Lock()
			defer consistentUpdated.L.Unlock()
			consistentPoint = stamp.(intStamp)
			consistentUpdated.Broadcast()
		},
		16,        // shards
		1024*1024, // backpressure bytes chosen to force delays
	)
	defer cancel()
	if !a.NoError(err) {
		return
	}

	// Generate data to be applied.
	waitFor, err := generateMutations(ctx, imm, tbl.Name(), 1024)
	if !a.NoError(err) {
		return
	}

	// Wait for the data to land, but also respect test timeout.
	consistentUpdated.L.Lock()
	for consistentPoint != waitFor {
		if !a.NoError(ctx.Err()) {
			return
		}
		consistentUpdated.Wait()
	}
	consistentUpdated.L.Unlock()
}

// This validates that the consistent point will still advance, even
// if a bucket is being continuously hammered with updates.
func TestBucketSaturation(t *testing.T) {
	a := assert.New(t)

	ctx, dbInfo, cancel := sinktest.Context()
	defer cancel()

	dbName, cancel, err := sinktest.CreateDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	watchers, cancel := schemawatch.NewWatchers(dbInfo.Pool())
	defer cancel()

	tbl, err := sinktest.CreateTable(ctx, dbName,
		"CREATE TABLE %s (k INT PRIMARY KEY, v STRING)")
	if !a.NoError(err) {
		return
	}

	appliers, cancel := apply.NewAppliers(watchers)
	defer cancel()

	// Provide a correct way for the callback to report where the
	// consistent point has advanced to.
	consistentUpdated := sync.NewCond(&sync.Mutex{})
	consistentCallbacks := 0

	imm, cancel, err := New(
		appliers,
		2*time.Minute,
		dbInfo.Pool(),
		func(stamp stamp.Stamp) {
			consistentUpdated.L.Lock()
			defer consistentUpdated.L.Unlock()
			consistentCallbacks++
			consistentUpdated.Broadcast()
		},
		16,        // shards
		1024*1024, // backpressure bytes chosen to force delays
	)
	defer cancel()
	if !a.NoError(err) {
		return
	}

	// Generate data to be applied, in a separate goroutine.
	go func() {
		_, _ = generateMutations(ctx, imm, tbl.Name(), math.MaxInt)
	}()

	// Ensure that several updates to the consistent point happen.
	consistentUpdated.L.Lock()
	for consistentCallbacks < 10 {
		consistentUpdated.Wait()
	}
	consistentUpdated.L.Unlock()
}

func generateMutations(
	ctx context.Context, imm *Fan, tbl ident.Table, batchCount int,
) (intStamp, error) {
	id := 0
	for batchID := 0; batchID < batchCount; batchID++ {
		muts := make([]types.Mutation, 10)
		for idx := range muts {
			id++
			muts[idx].Key = []byte(fmt.Sprintf(`[%d]`, id))
			muts[idx].Data = []byte(fmt.Sprintf(`{"k":%d,"v":"%d"}`, id, id))
		}

		if err := imm.Enqueue(ctx, intStamp(batchID), tbl, muts); err != nil {
			return 0, err
		}
		if batchID%10 == 0 {
			if err := imm.Mark(intStamp(batchID)); err != nil {
				return 0, err
			}
		}
	}
	// Ensure that we close out the last batch with an explicit mark.
	lastStamp := intStamp(batchCount - 1)
	return lastStamp, imm.Mark(lastStamp)
}
