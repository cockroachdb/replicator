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

package cdc

import (
	"encoding/json"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	"github.com/pkg/errors"
)

// resolvedStamp tracks the progress of applying staged mutations for
// a given resolved timestamp.
// Partial progress of very large batches is maintained in the staging
// database by tracking a per-mutation applied flag.
type resolvedStamp struct {
	// A resolved timestamp that represents a transactionally-consistent
	// point in the history of the workload.
	CommittedTime hlc.Time `json:"c,omitempty"`
	// Iteration is used to provide well-ordered behavior within a
	// single backfill window. Partial progress is maintained in the
	// staging tables in case cdc-sink is interrupted in the middle
	// of processing a large window.
	Iteration int `json:"i,omitempty"`
	// LargeBatchOffset allows us to jump over already-applied keys when
	// processing a large batch of data (e.g. an initial backfill). This
	// is merely a performance optimization, since the staging tables
	// are used to track whether or not any particular mutation has been
	// applied.
	LargeBatchOffset *ident.TableMap[json.RawMessage] `json:"off,omitempty"`
	// The next resolved timestamp that we want to advance to.
	ProposedTime hlc.Time `json:"p,omitempty"`
}

// AsTime implements logical.TimeStamp to improve reporting.
func (s *resolvedStamp) AsTime() time.Time {
	// Use the older time when backfilling.
	return time.Unix(0, s.CommittedTime.Nanos())
}

// Less implements stamp.Stamp.
func (s *resolvedStamp) Less(other stamp.Stamp) bool {
	o := other.(*resolvedStamp)
	if c := hlc.Compare(s.CommittedTime, o.CommittedTime); c != 0 {
		return c < 0
	}
	if c := hlc.Compare(s.ProposedTime, o.ProposedTime); c != 0 {
		return c < 0
	}
	return s.Iteration < o.Iteration
}

// NewCommitted returns a new resolvedStamp that represents the
// completion of a resolved timestamp.
func (s *resolvedStamp) NewCommitted() (*resolvedStamp, error) {
	if s.ProposedTime == hlc.Zero() {
		return nil, errors.New("cannot make new committed timestamp without proposed value")
	}

	return &resolvedStamp{CommittedTime: s.ProposedTime}, nil
}

// NewProposed returns a new resolvedStamp that extends the existing
// stamp with a later proposed timestamp.
func (s *resolvedStamp) NewProposed(proposed hlc.Time) (*resolvedStamp, error) {
	if hlc.Compare(proposed, s.CommittedTime) < 0 {
		return nil, errors.Errorf("proposed cannot roll back committed time: %s vs %s",
			proposed, s.CommittedTime)
	}
	if hlc.Compare(proposed, s.ProposedTime) < 0 {
		return nil, errors.Errorf("proposed time cannot go backward: %s vs %s", proposed, s.ProposedTime)
	}

	return &resolvedStamp{
		CommittedTime: s.CommittedTime,
		Iteration:     s.Iteration + 1,
		ProposedTime:  proposed,
	}, nil
}

// NewProgress returns a resolvedStamp that represents partial progress
// within the same [committed, proposed] window.
func (s *resolvedStamp) NewProgress(cursor *types.UnstageCursor) *resolvedStamp {
	ret := &resolvedStamp{
		CommittedTime: s.CommittedTime,
		Iteration:     s.Iteration + 1,
		ProposedTime:  s.ProposedTime,
	}

	// Record offsets to allow us to skip rows within the next query.
	if cursor != nil && cursor.StartAfterKey.Len() > 0 {
		ret.LargeBatchOffset = &ident.TableMap[json.RawMessage]{}
		cursor.StartAfterKey.CopyInto(ret.LargeBatchOffset)
	}

	return ret
}

// String is for debugging use only.
func (s *resolvedStamp) String() string {
	ret, _ := json.Marshal(s)
	return string(ret)
}
