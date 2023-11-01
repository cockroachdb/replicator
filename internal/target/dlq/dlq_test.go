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

package dlq_test

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/sinktest/all"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/stretchr/testify/require"
)

// TestDLQ verifies that the suggested DLQ schema can be created and
// inserts a few rows to smoke-check the query.
func TestDLQ(t *testing.T) {
	r := require.New(t)

	fixture, err := all.NewFixture(t)
	r.NoError(err)

	ctx := fixture.Context

	// We need to create the DLQ table. This is something that could be
	// done automatically, but see the discussion in dlq_schema.go.
	dlTable, err := fixture.CreateDLQTable(ctx)
	r.NoError(err)

	out, err := fixture.DLQs.Get(ctx, fixture.TargetSchema.Schema(), "my_dlq")
	r.NoError(err)

	muts := []types.Mutation{
		{
			Before: []byte(`{"pk":0, "before":true}`),
			Data:   []byte(`{"pk":0, "after":true}`),
			Key:    []byte("[ 0 ]"),
			Time:   hlc.New(123, 456),
		},
		{
			Data: []byte(`{"pk":0, "after":true}`),
			Key:  []byte("[ 1 ]"),
			Time: hlc.New(123, 456),
		},
	}

	for _, mut := range muts {
		r.NoError(out.Enqueue(ctx, fixture.TargetPool.DB, mut))
	}

	var ct int
	r.NoError(fixture.TargetPool.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM %s", dlTable)).Scan(&ct))
	r.Equal(len(muts), ct)
}

// TestMissingColumns verifies the error-reporting behavior if the DLQ
// table exists, but does not contain the required columns.
func TestMissingColumns(t *testing.T) {
	r := require.New(t)

	fixture, err := all.NewFixture(t)
	r.NoError(err)

	ctx := fixture.Context

	// We need to create the DLQ table. This is something that could be
	// done automatically, but see the discussion in dlq_schema.go.
	_, err = fixture.TargetPool.ExecContext(ctx,
		fmt.Sprintf("CREATE TABLE %s (pk INT PRIMARY KEY)",
			ident.NewTable(fixture.TargetSchema.Schema(), fixture.DLQConfig.TableName)))
	r.NoError(err)
	r.NoError(fixture.Watcher.Refresh(ctx, fixture.TargetPool))

	_, err = fixture.DLQs.Get(ctx, fixture.TargetSchema.Schema(), "foo")

	r.ErrorContains(err, "missing the following columns: "+
		"dlq_name, source_nanos, source_logical, data_after, data_before")
}

// TestMissingTable verifies the error-reporting behavior if the DLQ
// table does not exist at all.
func TestMissingTable(t *testing.T) {
	r := require.New(t)

	fixture, err := all.NewFixture(t)
	r.NoError(err)

	ctx := fixture.Context
	_, err = fixture.DLQs.Get(ctx, fixture.TargetSchema.Schema(), "foo")

	r.ErrorContains(err, "must be created")
}
