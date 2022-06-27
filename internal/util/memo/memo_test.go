// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package memo

import (
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/target/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/stretchr/testify/assert"
)

func TestRoundtrip(t *testing.T) {
	a := assert.New(t)

	fixture, cancel, err := sinktest.NewFixture()
	if !a.NoError(err) {
		return
	}
	defer cancel()

	ctx := fixture.Context
	dbName := fixture.TestDB.Ident()
	pool := fixture.Pool

	tgt := ident.NewTable(dbName, ident.Public, ident.New("t"))
	memo, err := New(ctx, pool, tgt)
	if !a.NoError(err) {
		return
	}
	tests := []struct {
		name   string
		key    string
		value  []byte
		insert bool
	}{
		{"value", "one", []byte("value"), true},
		{"empty", "two", []byte(""), true},
		{"default", "three", []byte("default"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.insert {
				err := memo.Put(ctx, pool, tt.key, tt.value)
				if !a.NoError(err) {
					return
				}
			}
			got, err := memo.Get(ctx, pool, tt.key, []byte("default"))
			if !a.NoError(err) {
				return
			}
			a.Equalf(tt.value, got, "%s failed", tt.name)

		})
	}
}
