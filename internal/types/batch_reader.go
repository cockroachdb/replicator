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

package types

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/util/hlc"
)

// BatchCursor is emitted by [BatchReader.Read].
type BatchCursor struct {
	// A batch of data, corresponding to a transaction in the source
	// database. This may be nil for a progress-only update.
	Batch *TemporalBatch

	// This field will be populated if the reader encounters an
	// unrecoverable error while processing. A result containing an
	// error will be the final message in the channel before it is
	// closed.
	Error error

	// Fragment will be set if the Batch is not guaranteed to contain
	// all data for its given timestamp. This will occur, for example,
	// if the number of mutations for the timestamp exceeds
	// [StagingQuery.FragmentSize] or if an underlying database query is
	// not guaranteed to have yet read all values at the batch's
	// time (e.g. scan boundary alignment). Consumers that require
	// transactionally-consistent views of the data should wait for the
	// next, non-fragmented, cursor update.
	Fragment bool

	// Jump indicates that the scanning bounds changed such that the
	// data in the stream may be disjoint.
	Jump bool

	// Marker is a caller-specific value associated with the cursor.
	Marker any

	// Progress indicates the range of data which has been successfully
	// scanned so far. Receivers may encounter progress-only updates
	// which happen when the end of the scanned bounds have been reached
	// or if there is a "pipeline bubble" when reading data from the
	// staging tables.
	Progress hlc.Range
}

// Copy returns a deep copy of the cursor.
func (c *BatchCursor) Copy() *BatchCursor {
	cpy := *c
	if c.Batch != nil {
		c.Batch = c.Batch.Copy()
	}
	return &cpy
}

// String is for debugging use only.
func (c *BatchCursor) String() string {
	var buf strings.Builder
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", " ")
	if err := enc.Encode(c); err != nil {
		return fmt.Sprintf("error: %v", err)
	}
	return buf.String()
}

// A BatchReader provides access to transactional data. Because this
// can be a potentially expensive or otherwise unbounded amount of data,
// the results are provided via a channel which may be incrementally
// consumed from buffered data.
type BatchReader interface {
	// Read returns a channel that will emit transactional data.
	// Implementations should be prepared to have Read called more than
	// once on the same BatchReader instance. In this case, the reader
	// should replay from an appropriate point.
	//
	// Care should be taken to [stopper.Context.Stop] the context passed
	// into this method to prevent goroutine or database leaks. When the
	// context is gracefully stopped, the channel will be closed
	// normally.
	//
	// Any errors encountered while reading will be returned in the
	// final message before closing the channel.
	Read(ctx *stopper.Context) (<-chan *BatchCursor, error)
}
