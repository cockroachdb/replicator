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

package types

import (
	"context"

	"github.com/cockroachdb/cdc-sink/internal/util/ident"
)

// A Conflict contains a mutation that was attempted and the existing
// data which blocked it. The maps in this type contain reified JSON
// representation of the values that one would expect to see from the
// json package.
type Conflict struct {
	// The mutation that cannot be applied. This reference exists to
	// provide access to the metadata. The other fields in this type are
	// the preferred sources of truth for values to operate on.
	Mutation Mutation

	Before   *ident.Map[any] // May be nil if only 2-way merge.
	Existing *ident.Map[any] // The data which blocked the attempt.
	Proposed *ident.Map[any] // The desired end state of the mutation.
}

// A Resolution contains the contents of a mutation after the Merger has
// resolved the data conflicts. Only one of the fields in this struct
// should be non-zero.
type Resolution struct {
	Apply *ident.Map[any] // The contents of the mutation to apply.
	DLQ   string          // Write the incoming mutation to the named queue.
}

// Merger resolves data conflicts.
type Merger interface {
	// Merge resolves a data conflict and returns a mutation that should
	// be unconditionally applied. If the returned boolean value is
	// false, the conflict should be ignored. It is the responsibility
	// of the Merger to ensure that the conflicted value arrived
	// somewhere.
	Merge(context.Context, TargetQuerier, *Conflict) (*Resolution, bool, error)
}

// MergeFunc adapts a function type to implement Merger.
type MergeFunc func(ctx context.Context, tx TargetQuerier, conflict *Conflict) (*Resolution, bool, error)

var _ Merger = MergeFunc(nil)

// Merge implements Merger.
func (fn MergeFunc) Merge(
	ctx context.Context, tx TargetQuerier, conflict *Conflict,
) (*Resolution, bool, error) {
	return fn(ctx, tx, conflict)
}
