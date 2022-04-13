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

func TestMemo_Roundtrip(t *testing.T) {
	a := assert.New(t)

	ctx, info, cancel := sinktest.Context()
	defer cancel()

	dbName, cancel, err := sinktest.CreateDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	pool := info.Pool()
	tgt := ident.NewTable(dbName, ident.Public, ident.New("t"))
	memo, err := NewMemo(ctx, pool, tgt)
	if !a.NoError(err) {
		return
	}
	tests := []struct {
		name   string
		key    string
		value  string
		insert bool
	}{
		{"value", "one", "value", true},
		{"empty", "two", "", true},
		{"default", "three", "default", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.insert {
				err := memo.Put(ctx, tt.key, tt.value)
				if !a.NoError(err) {
					return
				}
			}
			got, err := memo.Get(ctx, tt.key, "default")
			if !a.NoError(err) {
				return
			}
			a.Equalf(tt.value, got, "%s failed", tt.name)

		})
	}
}
