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
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
)

// Backfiller is an optional capability interface for Dialect
// implementations. The BackfillInto method will be called instead of
// ReadInto when the logical loop has detected a backfill state. The
// primary distincting between BackfillInto and ReadInto is that
// backfilling is a finite process.
type Backfiller interface {
	Dialect

	// BackfillInto represents a potentially-fragile source of
	// logical-replication messages that should be applied in a
	// high-throughput manner. Implementations should treat BackfillInto
	// as a signal to "catch up" with replication and then return once
	// the backfill process has completed or when [State.ShouldStop]
	// returns true.
	//
	// See also discussion on Dialect.ReadInto.
	BackfillInto(ctx context.Context, ch chan<- Message, state State) error
}

// ConsistentCallback is an optional interface that may be implemented
// by a Dialect.
type ConsistentCallback interface {
	// OnConsistent will be called whenever the Dialect's logical loop
	// has advanced to a new consistent point. This callback will block
	OnConsistent(cp stamp.Stamp) error
}

// Dialect encapsulates the source-specific implementation details.
type Dialect interface {
	// ReadInto represents a potentially-fragile source of
	// logical-replication messages.
	//
	// If this method returns with an error, a Rollback message will be
	// injected into the channel given to Process. ReadInto will then be
	// called again to restart the replication feed from the most recent
	// consistent point.
	//
	// The state argument provides the last consistent point that was
	// processed by the stream. This can be used to verify successful
	// resynchronization with the source database.
	ReadInto(ctx context.Context, ch chan<- Message, state State) error

	// Process decodes the logical replication messages, to call the
	// various Events methods. Implementations of Process should exit
	// gracefully when the channel is closed, this may represent a
	// switch from backfilling to a streaming mode. If this method
	// returns an error, the entire replication loop will be restarted.
	Process(ctx context.Context, ch <-chan Message, events Events) error

	// ZeroStamp constructs a new, zero-valued stamp that represents
	// a consistent point at the beginning of the source's history.
	ZeroStamp() stamp.Stamp
}

// Lessor is an optional Dialect capability when the Dialect requires an
// external lock to ensure correctness if multiple instances of cdc-sink
// are running.
type Lessor interface {
	// Acquire should return a lease used to control when a specific
	// replication loop is allowed to run. A error of
	// [types.LeaseBusyError] will trigger a sleep behavior before
	// attempting to reacquire the lease.
	Acquire(ctx context.Context) (types.Lease, error)
}

// A Message is specific to a Dialect.
type Message any

// Rollback is a sentinel message that will be injected into the values
// received by Dialect.Process.
var msgRollback Message = &struct{}{}

// IsRollback returns true if the message represents a break in the data
// emitted from Dialect.ReadInto.
func IsRollback(m Message) bool {
	return m == msgRollback
}

// Events extends State to drive the state of the replication loop.
type Events interface {
	State
	// Backfill will execute a single pass of the given Backfiller in a
	// blocking fashion. This is useful when sources are discovered
	// dynamically.
	Backfill(ctx context.Context, loopName string, backfiller Backfiller, options ...Option) error
	// Flush can be called after OnData() to ensure that any writes
	// have been flushed to the database. This is necessary when
	// using foreign keys and fan mode.
	Flush(ctx context.Context) error
	// OnBegin denotes the beginning of a transactional block in the
	// underlying logical feed.
	OnBegin(ctx context.Context, point stamp.Stamp) error
	// OnCommit denotes the end of a transactional black in the underlying
	// logical feed.
	OnCommit(ctx context.Context) error
	// OnData adds data to the transaction block. The source is a name
	// to pass to the user-script, and will generally be the name of a
	// table, doc-collection, or other named data product.
	OnData(ctx context.Context, source ident.Ident, target ident.Table, muts []types.Mutation) error
	// OnRollback must be called by Dialect.Process when a rollback
	// message is encountered, to ensure that all internal state has
	// been resynchronized.
	OnRollback(ctx context.Context, msg Message) error

	// drain is called after any attempt to run a replication loop.
	// Implementations should block until any pending mutations have
	// been committed.
	drain(ctx context.Context) error
}

// AwaitComparison is used with [State.AwaitConsistentPoint].
type AwaitComparison int

const (
	// AwaitGTE waits until the consistent point is greater than or
	// equal to some other point.
	AwaitGTE AwaitComparison = 0
	// AwaitGT waits until the consistent point is greater than some
	// other point.
	AwaitGT AwaitComparison = 1
)

// State provides information about a replication loop.
type State interface {
	// GetConsistentPoint returns the most recent consistent point that
	// has been committed to the target database or the value returned
	// from Dialect.ZeroStamp.
	GetConsistentPoint() stamp.Stamp
	// GetTargetDB returns the target database name.
	GetTargetDB() ident.Ident
	// NotifyConsistentPoint returns a channel that emits the next
	// consistent point that satisfies the comparison with the given
	// stamp. If the context is cancelled, the channel will be closed
	// without emitting a value.
	NotifyConsistentPoint(ctx context.Context, comparison AwaitComparison, point stamp.Stamp) <-chan stamp.Stamp
	// Stopping returns a channel that will be closed to allow for
	// graceful draining or to switch in and out of backfill mode. This
	// should be checked  on occasion by [Dialect.ReadInto] and
	// [Backfiller.BackfillInto].
	Stopping() <-chan struct{}
}

// OffsetStamp is a Stamp which can represent itself as an absolute
// offset value. This is used for optional metrics reporting.
type OffsetStamp interface {
	stamp.Stamp
	AsOffset() uint64
}

// TimeStamp is a Stamp which can represent itself as a time.Time. This
// is used to enable backfill mode and for metrics reporting.
type TimeStamp interface {
	stamp.Stamp
	AsTime() time.Time
}
