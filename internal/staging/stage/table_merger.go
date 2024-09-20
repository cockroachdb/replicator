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

package stage

import (
	"math"
	"time"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// A mergeBuffer holds working state received from an underlying
// tableReader channel in order to join mutations by timestamp.
type mergeBuffer struct {
	buffer *tableCursor        // Data from the upstream channels
	offset int                 // Index into buffered TemporalBatches
	source <-chan *tableCursor // Upstream data.
}

// Empty returns true when the buffer has no more useful data.
func (s *mergeBuffer) Empty() bool {
	return s.buffer == nil || s.offset >= len(s.buffer.Batch.Data)
}

// Fragment returns the fragment bit of the current buffer.
func (s *mergeBuffer) Fragment() bool {
	return s.buffer != nil && s.buffer.Fragment
}

// Jump returns the jump bit of the current buffer.
func (s *mergeBuffer) Jump() bool {
	return s.buffer != nil && s.buffer.Jump
}

// Take dequeues a single-table, single-timestamp batch of data. This
// method will return nil if the buffer is empty.
func (s *mergeBuffer) Take() *types.TemporalBatch {
	if s.Empty() {
		return nil
	}
	ret := s.buffer.Batch.Data[s.offset]
	s.offset++
	return ret
}

// Progress returns the time of the mutation that would be next to be
// dequeued, or the scan's end time, if the buffer has been emptied.
func (s *mergeBuffer) Progress() hlc.Range {
	if s.buffer == nil {
		return hlc.RangeEmpty()
	}
	if s.Empty() {
		return s.buffer.Progress
	}
	return hlc.RangeExcluding(hlc.Zero(), s.buffer.Batch.Data[s.offset].Time)
}

// Refresh the buffer from the underlying channel. If the context is
// nil, the refresh will be non-blocking.
func (s *mergeBuffer) Refresh(ctx *stopper.Context) error {
	if ctx == nil {
		select {
		case cursor, open := <-s.source:
			if !open {
				return nil
			}
			return s.ingest(cursor)
		default:
			return nil
		}
	}
	select {
	case cursor, open := <-s.source:
		if !open {
			return nil
		}
		return s.ingest(cursor)
	case <-ctx.Stopping():
		return nil
	}
}

// ingest copies the state from the cursor notification. It is called by
// Refresh.
func (s *mergeBuffer) ingest(cursor *tableCursor) error {
	if cursor.Error != nil {
		return cursor.Error
	}
	s.buffer = cursor
	s.offset = 0
	return nil
}

// A tableMerger implements a "GROUP BY ORDER BY" operation on mutations
// to merge multiple channels of table data into a consistent view.
type tableMerger struct {
	group  *types.TableGroup
	out    chan<- *types.BatchCursor
	states []*mergeBuffer

	mergeLag   prometheus.Gauge
	mergeQueue prometheus.Gauge
}

// newTableMerger initializes the internal state of a tableMerger.
func newTableMerger(
	group *types.TableGroup, sources []<-chan *tableCursor, out chan<- *types.BatchCursor,
) *tableMerger {
	labels := metrics.SchemaValues(group.Enclosing)
	ret := &tableMerger{
		group:  group,
		states: make([]*mergeBuffer, len(sources)),
		out:    out,

		mergeLag:   stageMergeLag.WithLabelValues(labels...),
		mergeQueue: stageMergeQueue.WithLabelValues(labels...),
	}
	for idx, source := range sources {
		ret.states[idx] = &mergeBuffer{
			source: source,
		}
	}
	return ret
}

// run assumes it's being executed from its own goroutine. It will close
// the output channel when it exits.
func (m *tableMerger) run(ctx *stopper.Context) {
	defer close(m.out)

	// Initialize all states.
	for _, state := range m.states {
		if err := state.Refresh(ctx); err != nil {
			select {
			case m.out <- &types.BatchCursor{Error: err}:
			case <-ctx.Stopping():
			}
			return
		}
	}

	for {
		// Determine the next update and send it.
		cursor, refill, blocking := m.nextStep()

		// Cursor will be nil if a refresh is required to proceed.
		if cursor != nil {
			// Set queue-depth metric and lag before blocking.
			m.mergeLag.Set(float64(time.Now().UnixNano()-
				cursor.Progress.MaxInclusive().Nanos()) / 1e9)
			m.mergeQueue.Set(float64(len(m.out)))

			select {
			case m.out <- cursor:
				// An error message is terminal.
				if cursor.Error != nil {
					return
				}
			case <-ctx.Stopping():
				return
			}
		}

		// Refill buffers after sending the message. The refill may be
		// proactive, or blocking.
		for _, buf := range refill {
			var err error
			if blocking {
				err = buf.Refresh(ctx)
			} else {
				err = buf.Refresh(nil)
			}
			if err != nil {
				select {
				case m.out <- &types.BatchCursor{Error: err}:
				case <-ctx.Stopping():
				}
				return
			}
		}
	}
}

// nextStep will find the event(s) with the lowest common timestamp
// to send.
func (m *tableMerger) nextStep() (cursor *types.BatchCursor, refill []*mergeBuffer, block bool) {
	// Find the buffer(s) with the least progress.
	minTime := hlc.New(math.MaxInt64, math.MaxInt)
	empty := make([]*mergeBuffer, 0, len(m.states))
	toSend := make([]*mergeBuffer, 0, len(m.states))
	for _, buf := range m.states {
		progress := buf.Progress()
		if c := hlc.Compare(progress.Max(), minTime); c == 0 {
			toSend = append(toSend, buf)
			if buf.Empty() {
				empty = append(empty, buf)
			}
		} else if c < 0 {
			// New, lower timestamp. Reset accumulators.
			minTime = progress.Max()
			toSend = toSend[:1]
			toSend[0] = buf

			if buf.Empty() {
				empty = empty[:1]
				empty[0] = buf
			} else {
				empty = empty[:0]
			}
		}
	}

	// We can always notify the consumer of progress made.
	cursor = &types.BatchCursor{Progress: hlc.RangeExcluding(hlc.Zero(), minTime)}

	// Some buffers are empty. Exit now to force a blocking refresh to
	// let db scans catch up.
	if len(empty) > 0 {
		// If every buffer is empty, we want to send a progress-only
		// notification and then force a refill operation on all
		// buffers. This will occur when we've read to the end of all
		// available data.
		if len(toSend) == len(empty) {
			return cursor, empty, true
		}
		// We're in a mixed-fill case at some non-maximal progress
		// timestamp. Let's say that B1 has data, but B2 is empty. We
		// know that B2 is still in the middle of filling; if B2 really
		// is empty then its progress timestamp should be greater than
		// B1's timestamp.
		return nil, empty, true
	}

	for _, buf := range toSend {
		// Propagate the discontinuity flag.
		cursor.Jump = cursor.Jump || buf.Jump()

		// Dequeue a batch from the buffer.
		batch := buf.Take()

		// Copy this batch into the outgoing cursor. Batch will be nil
		// on a progress-only update from the source.
		if batch != nil {
			if cursor.Batch == nil {
				cursor.Batch = batch
			} else {
				batch.Data.CopyInto(&cursor.Batch.Data)
			}
		}

		// Propagate fragment bit on a final update and enqueue it for a
		// proactive refill.
		if buf.Empty() {
			cursor.Fragment = cursor.Fragment || buf.Fragment()
			empty = append(empty, buf)
		}
	}
	return cursor, empty, false
}
