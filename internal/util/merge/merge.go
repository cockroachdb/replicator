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

// Package merge provides support for three-way merge operations.
package merge

import (
	"context"
	"encoding/json"

	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
)

// A Conflict contains a mutation that was attempted and the existing
// data which blocked it. The maps in this type contain reified JSON
// representation of the values that one would expect to see from the
// json package.
type Conflict struct {
	Before   *Bag // May be nil if only 2-way merge.
	Proposed *Bag // The proposed end state of the mutation.

	// Target will be populated with the values that were present in the
	// target database. That is, the result of the merge should be to
	// apply the difference between Before and Proposed to Target.
	Target *Bag

	// Unmerged is populated by [Standard.Merge] and can be presented
	// to its fallback merge function. This slice will be populated with
	// the properties that could not be automatically merged. That is:
	//   Before[prop] != Target[prop] && Before[prop] != Proposed[prop]
	Unmerged []ident.Ident
}

// ConflictError returns an error that provides details about a Conflict
// which could not be merged.
func ConflictError(con *Conflict) error {
	data, err := json.Marshal(con)
	if err != nil {
		return errors.Wrap(err, "error while describing unmerged conflict")
	}
	return errors.Errorf("unmerged data conflict: %s", string(data))
}

// A Resolution contains the contents of a mutation after the Merger has
// resolved the data conflicts. Exactly one of the fields in this struct
// should be non-zero.
type Resolution struct {
	Apply *Bag   // The contents of the mutation to apply.
	DLQ   string // Write the incoming mutation to the named queue.
	Drop  bool   // Discard the mutation.
}

// Merger resolves data conflicts.
type Merger interface {
	// Merge resolves a data conflict and returns a mutation that should
	// be unconditionally applied. If the returned boolean value is
	// false, the conflict should be ignored. It is the responsibility
	// of the Merger to ensure that the conflicted value arrived
	// somewhere.
	Merge(context.Context, *Conflict) (*Resolution, error)
}

// DLQ returns a Merger that sends all values to the named dead-letter
// queue. This can be used as the final stage of a merge pipeline.
func DLQ(name string) Merger {
	return Func(func(context.Context, *Conflict) (*Resolution, error) {
		return &Resolution{DLQ: name}, nil
	})
}

// Func adapts a function type to implement Merger.
type Func func(ctx context.Context, conflict *Conflict) (*Resolution, error)

var _ Merger = Func(nil)

// MarshalText implements [encoding.TextMarshaler] and is provided so
// that the Func will play nicely in the Diagnostics interface.
func (fn Func) MarshalText() ([]byte, error) {
	return []byte("merge.Func"), nil
}

// Merge implements Merger.
func (fn Func) Merge(ctx context.Context, conflict *Conflict) (*Resolution, error) {
	return fn(ctx, conflict)
}
