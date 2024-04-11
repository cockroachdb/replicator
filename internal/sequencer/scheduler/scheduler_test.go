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

package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/sinktest/mutations"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/lockset"
	"github.com/cockroachdb/cdc-sink/internal/util/notify"
	"github.com/stretchr/testify/require"
)

// This really just tests the key generation. See lockset tests for
// more in-depth testing of ordering guarantees.
func TestScheduler(t *testing.T) {
	r := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	muts := mutations.Generator(ctx, 1024, 0)
	table := ident.NewTable(ident.MustSchema(ident.New("schema")), ident.New("table"))

	batch := &types.MultiBatch{}
	tableBatch := &types.TableBatch{Table: table}
	var lastMut types.Mutation
	for i := 0; i < 100; i++ {
		lastMut = <-muts
		r.NoError(batch.Accumulate(table, lastMut))
		tableBatch.Data = append(tableBatch.Data, lastMut)
	}

	block := make(chan struct{})
	var seen []string
	locks, err := lockset.New[string](lockset.GoRunner(ctx), "testing")
	r.NoError(err)
	s := &Scheduler{locks}

	// This single task executes first, and blocks the following tasks.
	singleStatus := s.Singleton(table, lastMut, func() error {
		seen = append(seen, "single")
		<-block
		return nil
	})
	tableStatus := s.TableBatch(tableBatch, func() error {
		seen = append(seen, "table")
		return nil
	})
	temporalStatus := s.TemporalBatch(batch.Data[0], func() error {
		seen = append(seen, "temporal")
		return nil
	})
	batchStatus := s.Batch(batch, func() error {
		seen = append(seen, "batch")
		return nil
	})

	// Allow singleton to complete.
	close(block)

	// Await outcome.
	r.NoError(lockset.Wait(ctx, []*notify.Var[*lockset.Status]{
		singleStatus, tableStatus, temporalStatus, batchStatus,
	}))

	// Verify execution order.
	r.Equal([]string{"single", "table", "temporal", "batch"}, seen)
}
