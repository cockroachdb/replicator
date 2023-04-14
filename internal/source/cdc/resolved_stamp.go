// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cdc

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	"github.com/cockroachdb/cdc-sink/internal/util/txguard"
	"github.com/pkg/errors"
)

// resolvedStamp tracks the progress of applying staged mutations for
// a given resolved timestamp.  This type has some extra complexity to
// support backfilling of initial changefeed scans.
//
// Here's a state diagram to help:
//
//	committed
//	    |                *---*
//	    V                V   |
//	 proposed ---> backfill -*
//	    |                |
//	    V                |
//	committed <----------*
//
// Tracking of FK levels and table offsets is only used in backfill mode
// and is especially relevant to an initial_scan from a changefeed. In
// this state, we have an arbitrarily large number of mutations to
// apply, all of which will have the same timestamp.
//
// Each resolvedStamp is associated with a separate database transaction
// that holds a SELECT FOR UPDATE lock on the row that holds the
// timestamp being resolved. The ownership of this transaction is
// transferred to any derived resolvedStamp that represents partial
// progress within a resolved timestamp. The resolved-timestamp
// transaction is wrapped in a txguard.Guard to ensure that it is kept
// alive.  If the resolved-timestamp transaction fails for any reason,
// we can roll back to the previously committed stamp through the usual
// logical-loop error-handling code.
type resolvedStamp struct {
	Backfill bool `json:"b,omitempty"`
	// A resolved timestamp that represents a transactionally-consistent
	// point in the history of the workload.
	CommittedTime hlc.Time `json:"c,omitempty"`
	// Iteration is used to provide well-ordered behavior within a
	// single backfill window.
	Iteration int `json:"i,omitempty"`
	// The next resolved timestamp that we want to advance to.
	ProposedTime hlc.Time `json:"p,omitempty"`

	OffsetKey   json.RawMessage `json:"ok,omitempty"`
	OffsetTable ident.Table     `json:"otbl,omitempty"`
	OffsetTime  hlc.Time        `json:"ots,omitempty"`

	mu struct {
		sync.Mutex
		tx *txguard.Guard
	}
}

// AsTime implements logical.TimeStamp to improve reporting.
func (s *resolvedStamp) AsTime() time.Time {
	// Use the older time when backfilling.
	return time.Unix(0, s.CommittedTime.Nanos())
}

// Commit marks the resolved timestamp as resolved, if the transaction
// has not been handed off to another resolvedStamp. This handoff would
// occur when processing a resolved-timestamp interval that contains a
// large number of sub-batches (e.g. backfills, high-rate scenarios).
func (s *resolvedStamp) Commit(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var err error
	if tx := s.mu.tx; tx != nil {
		s.mu.tx = nil
		err = errors.WithStack(tx.Commit(ctx))
	}

	return err
}

// IsAlive returns any pending error from the transaction-keepalive
// loop or nil if the keepalive loop is still running.
func (s *resolvedStamp) IsAlive() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx := s.mu.tx
	if tx == nil {
		return errors.New("resolved-timestamp transaction owned by later stamp")
	}
	return tx.IsAlive()
}

// Less implements stamp.Stamp.
func (s *resolvedStamp) Less(other stamp.Stamp) bool {
	o := other.(*resolvedStamp)
	if c := hlc.Compare(s.CommittedTime, o.CommittedTime); c != 0 {
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

	return s.handoff(&resolvedStamp{CommittedTime: s.ProposedTime}), nil
}

// NewProposed returns a new resolvedStamp that extends the existing
// stamp with a later proposed timestamp.
func (s *resolvedStamp) NewProposed(tx *txguard.Guard, proposed hlc.Time) (*resolvedStamp, error) {
	if hlc.Compare(proposed, s.CommittedTime) < 0 {
		return nil, errors.Errorf("proposed cannot roll back committed time: %s vs %s",
			proposed, s.CommittedTime)
	}
	if hlc.Compare(proposed, s.OffsetTime) < 0 {
		return nil, errors.Errorf("proposed time undoing work: %s vs %s", proposed, s.OffsetTime)
	}
	if hlc.Compare(proposed, s.ProposedTime) < 0 {
		return nil, errors.Errorf("proposed time cannot go backward: %s vs %s", proposed, s.ProposedTime)
	}

	ret := &resolvedStamp{
		Backfill:      s.Backfill,
		CommittedTime: s.CommittedTime,
		Iteration:     s.Iteration + 1,
		OffsetKey:     s.OffsetKey,
		OffsetTable:   s.OffsetTable,
		OffsetTime:    s.OffsetTime,
		ProposedTime:  proposed,
	}
	// We don't call handoff here, since we have a new transaction.
	ret.mu.tx = tx
	return ret, nil
}

// NewProgress returns a resolvedStamp that represents partial progress
// within the same [committed, proposed] window.
func (s *resolvedStamp) NewProgress(cursor *types.SelectManyCursor) *resolvedStamp {
	ret := &resolvedStamp{
		Backfill:      s.Backfill,
		CommittedTime: s.CommittedTime,
		Iteration:     s.Iteration + 1,
		OffsetKey:     s.OffsetKey,
		OffsetTable:   s.OffsetTable,
		OffsetTime:    s.OffsetTime,
		ProposedTime:  s.ProposedTime,
	}

	if cursor != nil {
		ret.OffsetTime = cursor.OffsetTime

		// Only Key and Table make sense to retain in a backfill. For
		// transactional mode, we always want to restart at a specific
		// timestamp.
		if s.Backfill {
			ret.OffsetKey = cursor.OffsetKey
			ret.OffsetTable = cursor.OffsetTable
		}
	}

	return s.handoff(ret)
}

// Rollback aborts the enclosed transaction, if it exists. This method
// is safe to call on a nil receiver.
func (s *resolvedStamp) Rollback() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if tx := s.mu.tx; tx != nil {
		tx.Rollback()
		s.mu.tx = nil
	}
}

// String is for debugging use only.
func (s *resolvedStamp) String() string {
	ret, _ := json.Marshal(s)
	return string(ret)
}

// handoff transfers the transaction to another resolvedStamp and starts
// its keepalive loop. This method should be called before the next
// value has been made visible to any other callers.
func (s *resolvedStamp) handoff(next *resolvedStamp) *resolvedStamp {
	s.mu.Lock()
	defer s.mu.Unlock()

	if tx := s.mu.tx; tx != nil {
		next.mu.tx = tx
		s.mu.tx = nil
	}

	return next
}
