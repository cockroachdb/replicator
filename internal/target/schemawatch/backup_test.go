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
	b := &memoBackup{
		memo:        &memo.Memory{},
		stagingPool: nil,
	}

	schema := ident.MustSchema(ident.New("test"))
	schemaData := &types.SchemaData{Order: make([][]ident.Table, 3)}

	ctx := stopper.Background()
	r := require.New(t)

	err := b.backup(ctx, schema, schemaData)
	r.NoError(err)

	restored, err := b.restore(ctx, schema)
	r.NoError(err)
	r.Equal(restored, schemaData)
}

func TestBackup_Update(t *testing.T) {
	memory := &memo.Memory{
		WriteCounter: notify.VarOf(0),
	}
	b := &memoBackup{
		memo:        memory,
		stagingPool: nil,
	}

	schema := ident.MustSchema(ident.New("test"))
	schemaData := &types.SchemaData{Order: make([][]ident.Table, 3)}
	schemaData2 := &types.SchemaData{Order: make([][]ident.Table, 2)}

	ctx := stopper.WithContext(context.Background())
	defer ctx.Stop(0)
	r := require.New(t)

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
	r.Equal(schemaData2, restored)
}
