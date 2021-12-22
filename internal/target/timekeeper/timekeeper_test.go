// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package timekeeper

import (
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/target/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/stretchr/testify/assert"
)

// This test applies a sequence of timestamp puts to the same key.
func TestPut(t *testing.T) {
	a := assert.New(t)
	ctx, dbInfo, cancel := sinktest.Context()
	a.NotEmpty(dbInfo.Version())
	defer cancel()

	targetDB, cancel, err := sinktest.CreateDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	table := ident.NewTable(targetDB, ident.Public, ident.New("timestamps"))
	s, err := NewTimeKeeper(ctx, dbInfo.Pool(), table)
	if !a.NoError(err) {
		return
	}

	const count = 10
	prev := hlc.Zero()
	for i := 0; i <= count; i++ {
		next := hlc.New(int64(1000*i), i)
		found, err := s.Put(ctx,
			dbInfo.Pool(),
			ident.NewSchema(table.Database(), table.Schema()),
			next,
		)
		if !a.NoError(err) {
			return
		}
		a.Equal(prev, found)
		prev = next
	}

	a.Equal(int64(1000*count), prev.Nanos())
	a.Equal(count, prev.Logical())
}
