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
	"sync"
	"time"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/ident"
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

	ret, err := newStage(f.stop, f.db, f.stagingDB, table)
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

// Query implements types.Stagers.
func (f *factory) Query(ctx context.Context, q *types.StagingQuery) (types.BatchReader, error) {
	if q.Bounds == nil {
		return nil, errors.New("Bounds unset")
	}
	if q.FragmentSize == 0 {
		return nil, errors.New("FragmentSize unset")
	}
	if len(q.Group.Tables) == 0 {
		return nil, errors.New("Targets is empty")
	}
	return &stagingReader{f, q}, nil
}

type stagingReader struct {
	*factory
	*types.StagingQuery
}

var _ types.BatchReader = (*stagingReader)(nil)

func (r *stagingReader) Read(ctx *stopper.Context) (<-chan *types.BatchCursor, error) {
	// Ensure all staging tables exist.
	for _, table := range r.Group.Tables {
		if _, err := r.Get(ctx, table); err != nil {
			return nil, err
		}
	}

	// Set up a task to read data from each table.
	tableChans := make([]<-chan *tableCursor, len(r.Group.Tables))
	for idx, target := range r.Group.Tables {
		ch := make(chan *tableCursor, 2)
		tableChans[idx] = ch
		tableReader := newTableReader(
			r.Bounds, r.db, r.FragmentSize, ch, r.stagingDB, target)
		ctx.Go(func(ctx *stopper.Context) error {
			tableReader.run(ctx)
			return nil
		})
	}

	mergeChan := make(chan *types.BatchCursor, 2)
	merger := newTableMerger(r.Group, tableChans, mergeChan)
	ctx.Go(func(ctx *stopper.Context) error {
		defer ctx.Stop(time.Second)
		merger.run(ctx)
		return nil
	})
	return mergeChan, nil
}
