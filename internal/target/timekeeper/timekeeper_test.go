// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package timekeeper_test

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

	fixture, cancel, err := sinktest.NewFixture()
	if !a.NoError(err) {
		return
	}
	defer cancel()

	ctx := fixture.Context
	tk := fixture.TimeKeeper
	schema := ident.NewSchema(ident.New("some_db"), ident.New("some_schema"))

	const count = 10
	prev := hlc.Zero()
	for i := 0; i <= count; i++ {
		next := hlc.New(int64(1000*i), i)
		found, err := tk.Put(ctx,
			fixture.Pool,
			schema,
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

func BenchmarkTimekeeper(b *testing.B) {
	a := assert.New(b)

	fixture, cancel, err := sinktest.NewFixture()
	if err != nil {
		b.Fatal(err)
	}
	defer cancel()

	ctx := fixture.Context
	tk := fixture.TimeKeeper

	s := ident.NewSchema(ident.New("db"), ident.Public)
	var ts int64
	var logical int

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ts++
		logical++
		_, err := tk.Put(ctx, fixture.Pool, s, hlc.New(ts, logical))
		if !a.NoError(err) {
			return
		}
	}
}
