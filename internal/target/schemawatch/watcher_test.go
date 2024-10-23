// Copyright 2023 The Cockroach Authors
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

package schemawatch_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/replicator/internal/sinktest"
	"github.com/cockroachdb/replicator/internal/sinktest/all"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/retry"
	"github.com/stretchr/testify/require"
)

func TestWatch(t *testing.T) {
	r := require.New(t)

	fixture, err := all.NewFixtureWithRefresh(t, all.RefreshDelay(time.Second))
	r.NoError(err)

	ctx := fixture.Context
	w := fixture.Watcher

	w2, err := fixture.Watchers.Get(sinktest.JumbleSchema(fixture.TargetSchema.Schema()))
	r.NoError(err)
	r.Same(w, w2)

	// Bootstrap column.
	tblInfo, err := fixture.CreateTargetTable(ctx, "CREATE TABLE %s (pk INT PRIMARY KEY)")
	r.NoError(err)

	colDataVar, err := w.Watch(ctx, tblInfo.Name())
	r.NoError(err)
	colData, colDataChanged := colDataVar.Get()

	t.Run("smoke", func(t *testing.T) {
		r := require.New(t)

		r.Len(colData, 1)
		r.Equal("pk", colData[0].Name.Canonical().Raw())
	})

	// Check that we can retrieve a jumbled table name.
	t.Run("jumbled", func(t *testing.T) {
		r := require.New(t)
		jumbled := sinktest.JumbleTable(tblInfo.Name())

		jumbledData, err := w.Watch(ctx, jumbled)
		r.NoError(err)

		data, _ := jumbledData.Get()
		r.Len(data, 1)
		r.Equal("pk", data[0].Name.Canonical().Raw())
	})

	// Add a column and expect to see it.
	t.Run("add_column", func(t *testing.T) {
		r := require.New(t)
		switch fixture.TargetPool.Product {
		case types.ProductCockroachDB, types.ProductPostgreSQL:
			r.NoError(retry.Execute(ctx, fixture.TargetPool,
				fmt.Sprintf("ALTER TABLE %s ADD COLUMN v VARCHAR", tblInfo.Name())))
		case types.ProductMariaDB, types.ProductMySQL:
			r.NoError(retry.Execute(ctx, fixture.TargetPool,
				fmt.Sprintf("ALTER TABLE %s ADD COLUMN v VARCHAR(10)", tblInfo.Name())))
		case types.ProductOracle:
			r.NoError(retry.Execute(ctx, fixture.TargetPool,
				fmt.Sprintf("ALTER TABLE %s ADD (v INT)", tblInfo.Name())))

		default:
			r.FailNow("unsupported product")
		}

		select {
		case <-ctx.Done():
			r.FailNow("timed out waiting for channel data")
		case <-colDataChanged:
			colData, colDataChanged = colDataVar.Get()
			r.Len(colData, 2)
			r.Equal("pk", colData[0].Name.Canonical().Raw())
			r.Equal("v", colData[1].Name.Canonical().Raw())
		}
	})

	// Expect the slice to be zero-length if the table is dropped.
	t.Run("drop", func(t *testing.T) {
		r := require.New(t)
		r.NoError(tblInfo.DropTable(ctx))
		select {
		case <-ctx.Done():
			r.FailNow("timed out waiting for channel close")
		case <-colDataChanged:
			colData, colDataChanged = colDataVar.Get()
		}

		r.Empty(colData)
	})
}
