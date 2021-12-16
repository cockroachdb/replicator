// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package mutation

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/backend/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/sinktypes"
	"github.com/cockroachdb/cdc-sink/internal/util/batches"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/stretchr/testify/assert"
)

// TestPutAndDrain will insert and dequeue a batch of Mutations.
func TestPutAndDrain(t *testing.T) {
	a := assert.New(t)
	ctx, dbInfo, cancel := sinktest.Context()
	a.NotEmpty(dbInfo.Version())
	defer cancel()

	targetDB, cancel, err := sinktest.CreateDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	factory := New(dbInfo.Pool(), ident.StagingDB)

	dummyTarget := ident.NewTable(
		targetDB, ident.Public, ident.New("target"))

	s, err := factory.Get(ctx, dummyTarget)
	if !a.NoError(err) {
		return
	}
	a.NotNil(s)

	stagingTable := s.(*store).stage

	// Cook test data.
	total := 3 * batches.Size()
	muts := make([]sinktypes.Mutation, total)
	for i := range muts {
		muts[i] = sinktypes.Mutation{
			Data: []byte(fmt.Sprintf(`{"pk": %d}`, i)),
			Key:  []byte(fmt.Sprintf(`[%d]`, i)),
			Time: hlc.New(int64(1000*i), i),
		}
	}

	// Insert.
	a.NoError(s.Store(ctx, dbInfo.Pool(), muts))

	// Sanity-check table.
	count, err := sinktest.GetRowCount(ctx, dbInfo.Pool(), stagingTable)
	a.NoError(err)
	a.Equal(total, count)

	// Ensure that data insertion is idempotent.
	a.NoError(s.Store(ctx, dbInfo.Pool(), muts))

	// Sanity-check table.
	count, err = sinktest.GetRowCount(ctx, dbInfo.Pool(), stagingTable)
	a.NoError(err)
	a.Equal(total, count)

	// Dequeue.
	ret, err := s.Drain(ctx, dbInfo.Pool(),
		hlc.Zero(), hlc.New(int64(1000*total+1), 0))
	a.NoError(err)
	a.Len(ret, total)

	// Should be empty now.
	count, err = sinktest.GetRowCount(ctx, dbInfo.Pool(), stagingTable)
	a.NoError(err)
	a.Equal(0, count)
}
