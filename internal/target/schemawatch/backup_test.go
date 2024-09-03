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

package schemawatch

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/field-eng-powertools/stopvar"
	"github.com/cockroachdb/replicator/internal/staging/memo"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/stretchr/testify/require"
)

func TestBackup_BackupRestore(t *testing.T) {
	r := require.New(t)
	ctx := stopper.Background()

	schema := ident.MustSchema(ident.New("test"))
	schemaData, err := newSchemaData(schema, 11)
	r.NoError(err)

	b := &memoBackup{
		memo: &memo.Memory{},
	}

	err = b.backup(ctx, schema, schemaData)
	r.NoError(err)

	restored, err := b.restore(ctx, schema)
	r.NoError(err)

	// Serialization is tested elsewhere, so we just want to make
	// sure we got some data back.
	r.Equal(schemaData.Entire.Order, restored.Entire.Order)
}

func TestBackup_Update(t *testing.T) {
	r := require.New(t)
	memory := &memo.Memory{
		WriteCounter: notify.VarOf(0),
	}
	b := &memoBackup{
		memo:        memory,
		stagingPool: nil,
	}

	schema := ident.MustSchema(ident.New("test"))
	schemaData, err := newSchemaData(schema, 3)
	r.NoError(err)
	schemaData2, err := newSchemaData(schema, 2)
	r.NoError(err)

	ctx := stopper.WithContext(context.Background())
	defer ctx.Stop(0)

	nv := notify.VarOf(schemaData)

	beforeFirstUpdate, _ := memory.WriteCounter.Get()
	b.startUpdates(ctx, nv, schema)
	// startUpdates takes a backup of the initial value
	afterFirstUpdate, _ := stopvar.WaitForChange(ctx, beforeFirstUpdate, memory.WriteCounter)

	nv.Set(schemaData2)
	// after the value changes, it takes another backup
	stopvar.WaitForChange(ctx, afterFirstUpdate, memory.WriteCounter)

	restored, err := b.restore(ctx, schema)
	r.NoError(err)
	// Serialization is tested elsewhere, so we just want to make
	// sure we got some data back.
	r.Equal(schemaData2.Entire.Order, restored.Entire.Order)
}

func newSchemaData(schema ident.Schema, numTables int) (*types.SchemaData, error) {
	sd := &types.SchemaData{
		Columns: &ident.TableMap[[]types.ColData]{},
	}
	refs := &ident.TableMap[[]ident.Table]{}
	tables := make([]ident.Table, numTables)
	for i := range tables {
		table := ident.NewTable(schema, ident.New(fmt.Sprintf("table_%d", i)))

		// Add some fake column data to test serialization below.
		sd.Columns.Put(table, []types.ColData{
			{Name: ident.New("pk"), Primary: true, Type: "int"},
			{Name: ident.New("val"), Type: "varchar"},
		})

		var children []ident.Table
		for j := range i {
			if len(tables)%(j+1) == 0 {
				children = append(children, table)
			}
		}
		refs.Put(table, children)
		tables[i] = table
	}
	return sd, sd.SetDependencies(refs)
}
