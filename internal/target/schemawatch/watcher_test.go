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

	"github.com/cockroachdb/cdc-sink/internal/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/all"
	"github.com/cockroachdb/cdc-sink/internal/target/schemawatch"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/stretchr/testify/require"
)

func TestWatch(t *testing.T) {
	r := require.New(t)

	// Override the delay to exercise the background goroutine.
	const delay = time.Second
	*schemawatch.RefreshDelay = delay
	defer func() { *schemawatch.RefreshDelay = time.Minute }()

	fixture, cancel, err := all.NewFixture()
	r.NoError(err)
	defer cancel()

	ctx := fixture.Context
	dbName := fixture.TargetSchema.Schema()
	w := fixture.Watcher

	w2, err := fixture.Watchers.Get(ctx, sinktest.JumbleSchema(fixture.TargetSchema.Schema()))
	r.NoError(err)
	r.Same(w, w2)

	// Bootstrap column.
	tblInfo, err := fixture.CreateTargetTable(ctx, "CREATE TABLE %s (pk INT PRIMARY KEY)")
	r.NoError(err)

	ch, cancel, err := w.Watch(tblInfo.Name())
	r.NoError(err)
	defer cancel()

	t.Run("smoke", func(t *testing.T) {
		r := require.New(t)

		select {
		case <-ctx.Done():
			r.FailNow("timed out waiting for channel data")
		case data := <-ch:
			r.Len(data, 1)
			r.Equal("pk", data[0].Name.Canonical().Raw())
		}
	})

	// Check that we can retrieve a jumbled table name.
	t.Run("jumbled", func(t *testing.T) {
		r := require.New(t)
		jumbled := sinktest.JumbleTable(tblInfo.Name())

		ch, cancel, err := w.Watch(jumbled)
		r.NoError(err)
		defer cancel()

		select {
		case <-ctx.Done():
			r.FailNow("timed out waiting for channel data")
		case data := <-ch:
			r.Len(data, 1)
			r.Equal("pk", data[0].Name.Canonical().Raw())
		}
	})

	// Add a column and expect to see it.
	t.Run("add_column", func(t *testing.T) {
		r := require.New(t)
		switch fixture.TargetPool.Product {
		case types.ProductCockroachDB, types.ProductPostgreSQL:
			r.NoError(retry.Execute(ctx, fixture.TargetPool,
				fmt.Sprintf("ALTER TABLE %s ADD COLUMN v STRING", tblInfo.Name())))

		case types.ProductOracle:
			r.NoError(retry.Execute(ctx, fixture.TargetPool,
				fmt.Sprintf("ALTER TABLE %s ADD (v INT)", tblInfo.Name())))

		default:
			r.FailNow("unsupported product")
		}

		select {
		case <-ctx.Done():
			r.FailNow("timed out waiting for channel data")
		case data := <-ch:
			r.Len(data, 2)
			r.Equal("pk", data[0].Name.Canonical().Raw())
			r.Equal("v", data[1].Name.Canonical().Raw())
		}
	})

	// Expect the channel to close if the table is dropped.
	t.Run("drop", func(t *testing.T) {
		r := require.New(t)
		r.NoError(tblInfo.DropTable(ctx))
		select {
		case <-ctx.Done():
			r.FailNow("timed out waiting for channel close")
		case _, open := <-ch:
			r.False(open)
		}

		// Check that we error out quickly on unknown tables.
		ch, cancel, err = w.Watch(ident.NewTable(dbName, ident.New("blah")))
		r.Nil(ch)
		r.Nil(cancel)
		r.Error(err)
	})
}
