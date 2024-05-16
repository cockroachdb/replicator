// Copyright 2024 The Cockroach Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package mylogical

import (
	"encoding/json"
	"testing"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockAccumulator struct {
	mutations ident.Map[[]types.Mutation]
}

var _ types.Accumulator = &mockAccumulator{}

// Accumulate implements Accumulator.
func (m *mockAccumulator) Accumulate(table ident.Table, mut types.Mutation) error {
	mutations, ok := m.mutations.Get(table.Table())
	if !ok {
		mutations = make([]types.Mutation, 0)
	}
	mutations = append(mutations, mut)
	m.mutations.Put(table.Table(), mutations)
	return nil
}

func (m *mockAccumulator) compare(a *assert.Assertions, table ident.Table, muts []types.Mutation) {
	mutations, ok := m.mutations.Get(table.Table())
	if !ok {
		a.Failf("unknown table %s", table.Raw())
		return
	}
	if len(mutations) != len(muts) {
		a.Fail("mutations are not the same")
		return
	}
	for idx, mut := range mutations {
		a.Equal(muts[idx].Before, mut.Before)
		a.Equal(muts[idx].Data, mut.Data)
		a.Equal(muts[idx].Key, mut.Key)
		a.Equal(muts[idx].Time, mut.Time)
		a.NotNil(mut.Meta["mylogical"])
	}
}

// TestOnDataTuple verifies that incoming tuples are added to a batch
func TestOnDataTuple(t *testing.T) {
	r := require.New(t)
	consistentPoint, err := newConsistentPoint(mysql.MariaDBFlavor).parseFrom("1-1-1")
	ts := hlc.New(consistentPoint.AsTime().UnixNano(), 0)
	r.NoError(err)
	schema := ident.MustSchema(ident.Public)
	// Simple KV table
	kvTableID := uint64(0)
	kvTable := ident.NewTable(schema, ident.New("t1"))
	kvCols := []types.ColData{
		{Name: ident.New("k"), Primary: true, Type: "int"},
		{Name: ident.New("v"), Primary: false, Type: "int"},
	}
	// Table with no key
	noKeyID := uint64(1)
	noKeyTable := ident.NewTable(schema, ident.New("t2"))
	noKeyCols := []types.ColData{
		{Name: ident.New("v"), Primary: false, Type: "int"},
	}
	tables := []ident.Table{kvTable, noKeyTable}
	columns := &ident.TableMap[[]types.ColData]{}
	columns.Put(kvTable, kvCols)
	columns.Put(noKeyTable, noKeyCols)
	c := &conn{
		columns:             columns,
		nextConsistentPoint: consistentPoint,
		relations: map[uint64]ident.Table{
			kvTableID: kvTable,
			noKeyID:   noKeyTable,
		},
		target: schema,
	}
	tests := []struct {
		name      string
		tuple     *replication.RowsEvent
		operation mutationType
		wantMuts  []types.Mutation
		wantErr   string
	}{
		{
			name: "insert",
			tuple: &replication.RowsEvent{
				TableID: kvTableID,
				Rows: [][]any{
					{1, 10},
					{2, 20},
					{3, 30},
				},
			},
			operation: insertMutation,
			wantMuts: []types.Mutation{
				{
					Data: json.RawMessage(`{"k":1,"v":10}`),
					Key:  json.RawMessage(`[1]`),
					Time: ts,
				},
				{
					Data: json.RawMessage(`{"k":2,"v":20}`),
					Key:  json.RawMessage(`[2]`),
					Time: ts,
				},
				{
					Data: json.RawMessage(`{"k":3,"v":30}`),
					Key:  json.RawMessage(`[3]`),
					Time: ts,
				},
			},
		},
		{
			name: "insert no key",
			tuple: &replication.RowsEvent{
				TableID: noKeyID,
				Rows: [][]any{
					{1},
				},
			},
			operation: insertMutation,
			wantMuts: []types.Mutation{
				{
					Data: json.RawMessage(`{"v":1}`),
					Key:  json.RawMessage(`null`),
					Time: ts,
				},
			},
		},
		{
			name: "update",
			tuple: &replication.RowsEvent{
				TableID: kvTableID,
				Rows: [][]any{
					{1, 10},
					{1, 11},
					{2, 20},
					{2, 21},
				},
			},
			operation: updateMutation,
			wantMuts: []types.Mutation{
				{
					Data: json.RawMessage(`{"k":1,"v":11}`),
					Key:  json.RawMessage(`[1]`),
					Time: ts,
				},
				{
					Data: json.RawMessage(`{"k":2,"v":21}`),
					Key:  json.RawMessage(`[2]`),
					Time: ts,
				},
			},
		},
		{
			name: "update no key",
			tuple: &replication.RowsEvent{
				TableID: noKeyID,
				Rows: [][]any{
					{1},
					{1},
				},
			},
			operation: updateMutation,
			wantErr:   "only inserts supported with no key",
		},
		{
			name: "delete",
			tuple: &replication.RowsEvent{
				TableID: kvTableID,
				Rows: [][]any{
					{3, 2},
				},
			},
			operation: deleteMutation,
			wantMuts: []types.Mutation{
				{
					Key:  json.RawMessage(`[3]`),
					Time: ts,
				},
			},
		},
		{
			name: "invalid_row_size", // Verification for bug #858
			tuple: &replication.RowsEvent{
				TableID: kvTableID,
				Rows: [][]any{
					{1, 2, 3},
				},
			},
			operation: insertMutation,
			wantErr:   "unexpected number of columns",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := assert.New(t)
			batch := &mockAccumulator{}
			err := c.onDataTuple(batch, tt.tuple, tt.operation)
			if tt.wantErr != "" {
				a.ErrorContains(err, tt.wantErr)
				return
			}
			a.NoError(err)
			batch.compare(a, tables[tt.tuple.TableID], tt.wantMuts)
		})
	}
}
