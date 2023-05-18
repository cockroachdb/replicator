// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package memo_test

import (
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/sinktest/all"
	"github.com/stretchr/testify/assert"
)

func TestRoundtrip(t *testing.T) {
	fixture, cancel, err := all.NewFixture()
	if !assert.NoError(t, err) {
		return
	}
	defer cancel()

	ctx := fixture.Context
	memo := fixture.Memo
	pool := fixture.Pool

	tests := []struct {
		name     string
		key      string
		expected []byte
		insert   bool
	}{
		{"value", "one", []byte("value"), true},
		{"empty", "two", []byte(""), true},
		{"default", "three", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := assert.New(t)
			if tt.insert {
				err := memo.Put(ctx, pool, tt.key, tt.expected)
				if !a.NoError(err) {
					return
				}
			}
			got, err := memo.Get(ctx, pool, tt.key)
			if a.NoError(err) {
				a.Equal(tt.expected, got)
			}
		})
	}
}
