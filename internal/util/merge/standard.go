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

package merge

import (
	"context"
	"sort"

	"github.com/cockroachdb/replicator/internal/util/crep"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/pkg/errors"
)

// Standard implements a basic three-way merge operator.
type Standard struct {
	// The Fallback will be invoked if there were properties that could
	// not be merged.
	Fallback Merger
}

var _ Merger = (*Standard)(nil)

// Merge implements Merger.
func (s *Standard) Merge(ctx context.Context, con *Conflict) (*Resolution, error) {
	if err := merge(con); err != nil {
		return nil, err
	}

	// Ideal case, we were able to automatically merge the properties.
	if len(con.Unmerged) == 0 {
		return &Resolution{Apply: con.Target}, nil
	}

	// Ensure stable ordering.
	sort.Sort(ident.Idents(con.Unmerged))

	// If a fallback merger is available, delegate to it.
	if s.Fallback != nil {
		return s.Fallback.Merge(ctx, con)
	}

	// The merge failed and there's nowhere to store the data.
	return nil, ConflictError(con)
}

// undefined is a sentinel value that represents the absence of a
// value. We want to be able to distinguish between a property not
// being set and a property being set to a nil / NULL value.
type undefined struct{}

func (undefined) MarshalJSON() ([]byte, error) {
	return []byte(`{"__undefined__":true}`), nil
}

// merge computes the properties that have changed between
// [Conflict.Before] and [Conflict.Proposed]. These changes are then
// applied to [Conflict.Target], which will represent the state of row
// to apply to the target. If there are properties that cannot be
// merged, they will be added to [Conflict.Unmerged].
func merge(con *Conflict) error {
	// This case shouldn't happen, since we wouldn't have a conflict
	// unless there were an existing row. As an exercise in
	// completeness, we'll cover this case, since it's a trivial one.
	if con.Target == nil || con.Target.Len() == 0 {
		con.Target = con.Proposed
		return nil
	}
	if con.Proposed == nil {
		return errors.New("no proposed data")
	}
	// Before will be null if the conflict arose due to an insert. In
	// this case, the best that we can do is to present all proposed
	// properties as unresolved to the fallback merge function.
	if con.Before == nil {
		con.Before = NewBagFrom(con.Proposed)
	}

	// We want to iterate over all mapped and unmapped properties
	// that are defined within the Conflict.
	for prop := range allProperties(con).Keys() {
		// We want to be able to distinguish the tri-state of unset
		// versus set-to-null.
		before, beforeExists := con.Before.Get(prop)
		if !beforeExists {
			before = undefined{}
		}
		target, targetExists := con.Target.Get(prop)
		if !targetExists {
			target = undefined{}
		}
		proposed, proposedExists := con.Proposed.Get(prop)
		if !proposedExists {
			proposed = undefined{}
		}

		// We'll compare before and proposed to determine an action that
		// we may take. We need to use a somewhat fuzzy approach to
		// equality, since we could have varying in memory type from the
		// json package versus the database. For example, we could see
		// an untyped int versus an int64.
		isUnchanged, err := crep.Equal(before, proposed)
		if err != nil {
			return errors.Wrapf(err, "property: %s", prop)
		}

		// If the before and proposed values are the same, then we don't
		// need to do anything else with this property.
		if isUnchanged {
			continue
		}

		// If the proposed value already exists within the target, then
		// we can treat the update as a no-op.
		isIdempotent, err := crep.Equal(target, proposed)
		if err != nil {
			return errors.Wrapf(err, "property: %s", prop)
		}
		if isIdempotent {
			continue
		}

		// Now we need to determine if the proposed value is "safe" to
		// apply. The change will be safe if the target doesn't yet
		// define the property or if the target value matches the before
		// value.
		var isSafe bool
		if !targetExists {
			isSafe = true
		} else {
			isSafe, err = crep.Equal(before, target)
			if err != nil {
				return err
			}
		}

		// If the before and target values don't match, we'll record the
		// property name for later and keep processing.
		if !isSafe {
			con.Unmerged = append(con.Unmerged, prop)
			continue
		}

		// We have a change that's safe to make.
		if proposedExists {
			con.Target.Put(prop, proposed)
		} else {
			con.Target.Delete(prop)
		}
	}
	return nil
}

// allProperties accumulates a set of all actionable properties
// contained in the bags.
//
// The mapped properties are a function of the target database and will
// be the same between all bags. Unmapped properties could be disjoint,
// so we need to process all bags in the conflict.
//
// Primary key columns are ignored, since those values define the row
// identity. Ignored columns are ignored because ignorance is bliss.
func allProperties(con *Conflict) *ident.Map[struct{}] {
	ret := &ident.Map[struct{}]{}
	for _, col := range con.Proposed.Columns {
		if col.Primary || col.Ignored {
			continue
		}
		ret.Put(col.Name, struct{}{})
	}

	for prop := range con.Proposed.Unmapped.Keys() {
		ret.Put(prop, struct{}{})
	}
	for prop := range con.Before.Unmapped.Keys() {
		ret.Put(prop, struct{}{})
	}
	for prop := range con.Target.Unmapped.Keys() {
		ret.Put(prop, struct{}{})
	}
	return ret
}
