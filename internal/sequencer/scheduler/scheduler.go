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

// Package scheduler contains Sequencer-specific utilities for ensuring
// ordered access to rows.
package scheduler

import (
	"fmt"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/lockset"
)

// A Scheduler is shared across Sequencer implementations to limit
// overall parallelism and to ensure ordered access to target rows.
type Scheduler struct {
	set *lockset.Set[string]
}

// Batch executes the callback when it has clear access to apply
// all mutations in the given batch.
func (s *Scheduler) Batch(batch *types.MultiBatch, fn func() error) *notify.Var[*lockset.Status] {
	ret, _ := s.set.Schedule(
		keyForBatch[*types.MultiBatch](batch),
		func([]string) error {
			executingCount.Inc()
			defer executingCount.Dec()
			defer executedCount.Inc()
			return fn()
		},
	)
	return ret
}

// Singleton executes the callback executed when it has clear access to
// apply the mutation in the given table.
func (s *Scheduler) Singleton(
	table ident.Table, mut types.Mutation, fn func() error,
) *notify.Var[*lockset.Status] {
	ret, _ := s.set.Schedule(
		keyForSingleton(table, mut),
		func([]string) error {
			executingCount.Inc()
			defer executingCount.Dec()
			defer executedCount.Inc()
			return fn()
		},
	)
	return ret
}

// TableBatch executes the callback executed when it has clear access to
// apply the mutation in the given batch.
func (s *Scheduler) TableBatch(
	batch *types.TableBatch, fn func() error,
) *notify.Var[*lockset.Status] {
	ret, _ := s.set.Schedule(
		keyForBatch[*types.TableBatch](batch),
		func([]string) error {
			executingCount.Inc()
			defer executingCount.Dec()
			defer executedCount.Inc()
			return fn()
		},
	)
	return ret
}

// TemporalBatch executes the callback executed when it has clear access
// to apply the mutations in the given batch.
func (s *Scheduler) TemporalBatch(
	batch *types.TemporalBatch, fn func() error,
) *notify.Var[*lockset.Status] {
	ret, _ := s.set.Schedule(
		keyForBatch[*types.TemporalBatch](batch),
		func([]string) error {
			executingCount.Inc()
			defer executingCount.Dec()
			defer executedCount.Inc()
			return fn()
		},
	)
	return ret
}

// key returns a locking key for a single mutation.
func key(table ident.Table, mut types.Mutation) string {
	return fmt.Sprintf("%s:%s", table, mut.Key)
}

// keyForSingleton prevents collisions if the same key is operated on
// multiple times within a single sweep.
func keyForSingleton(table ident.Table, mut types.Mutation) []string {
	return []string{key(table, mut)}
}

// keyForBatch creates a locking set for all rows within the batch.
func keyForBatch[B types.Batch[B]](batch B) []string {
	var ret []string
	for table, mut := range batch.Mutations() {
		ret = append(ret, keyForSingleton(table, mut)...)
	}
	return ret
}
