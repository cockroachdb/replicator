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

package logical

import (
	"context"

	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
)

// fanEvents is a high-throughput implementation of Events which
// does not preserve transaction boundaries.
type fanEvents struct {
	loop *loop // The underlying loop.
}

var _ Events = (*fanEvents)(nil)

// Backfill implements Events. It delegates to the enclosing loop.
func (f *fanEvents) Backfill(ctx context.Context, source string, backfiller Backfiller) error {
	return f.loop.doBackfill(ctx, source, backfiller)
}

// GetConsistentPoint implements State. It delegates to the loop.
func (f *fanEvents) GetConsistentPoint() (stamp.Stamp, <-chan struct{}) {
	return f.loop.GetConsistentPoint()
}

// GetTargetDB implements State. It delegates to the loop.
func (f *fanEvents) GetTargetDB() ident.Schema { return f.loop.GetTargetDB() }

// OnBegin implements Events.
func (f *fanEvents) OnBegin(ctx context.Context) (Batch, error) {
	return newFanBatch(ctx, f.loop), nil
}

// SetConsistentPoint implements State.
func (f *fanEvents) SetConsistentPoint(ctx context.Context, cp stamp.Stamp) error {
	return f.loop.SetConsistentPoint(ctx, cp)
}

// Stopping implements State and delegates to the enclosing loop.
func (f *fanEvents) Stopping() <-chan struct{} {
	return f.loop.Stopping()
}
