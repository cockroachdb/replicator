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

package stage

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/pkg/errors"
)

type factory struct {
	db        *types.StagingPool
	stagingDB ident.Schema
	stop      *stopper.Context

	mu struct {
		sync.RWMutex
		instances *ident.TableMap[*stage]
	}
}

var _ types.Stagers = (*factory)(nil)

// Get returns a memoized instance of a stage for the given table.
func (f *factory) Get(_ context.Context, target ident.Table) (types.Stager, error) {
	if ret := f.getUnlocked(target); ret != nil {
		return ret, nil
	}
	return f.createUnlocked(target)
}

func (f *factory) createUnlocked(table ident.Table) (*stage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if ret := f.mu.instances.GetZero(table); ret != nil {
		return ret, nil
	}

	ret, err := newStore(f.stop, f.db, f.stagingDB, table)
	if err == nil {
		f.mu.instances.Put(table, ret)
	}
	return ret, err
}

func (f *factory) getUnlocked(table ident.Table) *stage {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.mu.instances.GetZero(table)
}

// Unstage implements types.Stagers.
func (f *factory) Unstage(
	ctx context.Context,
	tx types.StagingQuerier,
	cursor *types.UnstageCursor,
	fn types.UnstageCallback,
) (*types.UnstageCursor, bool, error) {
	// Duplicate the cursor so callers can choose to advance.
	cursor = cursor.Copy()

	q, err := newTemplateData(cursor, f.stagingDB).Eval()
	if err != nil {
		return nil, false, err
	}
	keyOffsets := make([]string, len(cursor.Targets))
	for idx, tbl := range cursor.Targets {
		keyOffsets[idx] = string(cursor.StartAfterKey.GetZero(tbl))
	}
	hadRows := false
	err = retry.Loop(ctx, func(ctx context.Context, sideEffect *retry.Marker) error {
		rows, err := tx.Query(ctx, q,
			cursor.StartAt.Nanos(),
			cursor.StartAt.Logical(),
			cursor.EndBefore.Nanos(),
			cursor.EndBefore.Logical(),
			keyOffsets)
		if err != nil {
			return errors.Wrap(err, q)
		}
		defer rows.Close()

		// We want to reset StartAfterKey whenever we read into a new region
		// of timestamps.
		epoch := cursor.StartAt
		for rows.Next() {
			var mut types.Mutation
			var tableIdx int
			var nanos int64
			var logical int
			if err := rows.Scan(&tableIdx, &nanos, &logical, &mut.Key, &mut.Data, &mut.Before); err != nil {
				return errors.WithStack(err)
			}

			// We generate observable side-effects below this point, so
			// we don't want to automatically retry.
			sideEffect.Mark()

			mut.Before, err = maybeGunzip(mut.Before)
			if err != nil {
				return err
			}
			mut.Data, err = maybeGunzip(mut.Data)
			if err != nil {
				return err
			}
			mut.Time = hlc.New(nanos, logical)
			if hlc.Compare(mut.Time, epoch) > 0 {
				epoch = mut.Time
				cursor.StartAt = epoch
				cursor.StartAfterKey = ident.TableMap[json.RawMessage]{}
			}
			cursor.StartAfterKey.Put(cursor.Targets[tableIdx], mut.Key)
			hadRows = true

			// No going back.
			if err := fn(ctx, cursor.Targets[tableIdx], mut); err != nil {
				return err
			}
		}
		if err := rows.Err(); err != nil {
			return errors.WithStack(err)
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}

	return cursor, hadRows, nil
}
