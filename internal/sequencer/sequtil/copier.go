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

package sequtil

import (
	"time"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	log "github.com/sirupsen/logrus"
)

// A step represents a single iteration of the copy loop.
type step struct {
	cursors        []*types.BatchCursor // May be nil for progress-only update.
	fragment       bool                 // Indicates a non-terminal segment of a multipart payload.
	progress       hlc.Range            // The range of data copied so far.
	reportProgress bool                 // Indicates that the progress callback is to be called.
}

// EachFn is a callback from a [Copier].
type EachFn func(ctx *stopper.Context, cursor *types.BatchCursor) error

// FlushFn is a callback from a [Copier].
type FlushFn func(ctx *stopper.Context, cursors []*types.BatchCursor, fragment bool) error

// ProgressFn is a callback from a [Copier].
type ProgressFn func(ctx *stopper.Context, progress hlc.Range) error

// A Copier consumes a channel of [types.BatchCursor], assembles the
// individual temporal batches into large batches, and invokes event
// callbacks to process the larger batches.
type Copier struct {
	Config   *sequencer.Config         // Controls for flush behavior.
	Each     EachFn                    // Optional callback to receive each batch.
	Flush    FlushFn                   // Optional callback to receive aggregated data.
	Progress ProgressFn                // Optional callback when the source has become idle.
	Source   <-chan *types.BatchCursor // Input data.
}

// Run copies data from the source to the target. It is a blocking call
// that will return if the context is stopped, the channel is closed, or
// if the target returns an error.
func (c *Copier) Run(ctx *stopper.Context) error {
	for {
		step, err := c.nextStep(ctx)
		if err != nil {
			return err
		}
		// Step will be nil if the context is being stopped.
		if step == nil {
			return nil
		}
		// Either or both fields may be set. The callbacks are optional.
		if len(step.cursors) > 0 && c.Flush != nil {
			log.Tracef("copier at: %s", step.progress)
			if err := c.Flush(ctx, step.cursors, step.fragment); err != nil {
				return err
			}
		}
		// Separate callback for progress-only updated.s
		if step.reportProgress && c.Progress != nil {
			log.Tracef("copier progress: %s", step.progress)
			if err := c.Progress(ctx, step.progress); err != nil {
				return err
			}
		}
	}
}

// nextStep will read from the source and decide what the next step
// should be. A nil step will be returned if the context is being
// stopped.
func (c *Copier) nextStep(ctx *stopper.Context) (*step, error) {
	// Ensure initial state, post-flush.
	var accumulator []*types.BatchCursor
	count := 0

	// Receiving from a nil channel blocks forever.
	var timerC <-chan time.Time

	// If we receive and buffer data, we'll loop around to here, rather
	// than exiting this method. Since we only start the timer after
	// there's some amount of data in the accumulator, we don't exit
	// this method until there's a complete action for the caller.
top:
	if count > 0 && c.Config.FlushPeriod > 0 && timerC == nil {
		timer := time.NewTimer(c.Config.FlushPeriod)
		defer timer.Stop()
		timerC = timer.C
	}

	select {
	case cursor, open := <-c.Source:
		// The channel has been closed. This will happen when the
		// stopper is being stopped.
		if !open {
			return nil, nil
		}
		// There was an error reading from staging.
		if cursor.Error != nil {
			return nil, cursor.Error
		}
		// We can always report on progress.
		ret := &step{progress: cursor.Progress}

		// We're receiving a progress notification cursor. This happens
		// when we've read to the end of the requested bounds or if
		// we're waiting on the db query buffers to refill. We'll take
		// this opportunity to flush data.
		batch := cursor.Batch
		if batch == nil {
			if count > 0 {
				ret.cursors = accumulator
			}
			// We want to provide data and call the report endpoint.
			ret.reportProgress = true
			return ret, nil
		}

		// Allow spying on data as it arrives.
		if c.Each != nil {
			if err := c.Each(ctx, cursor); err != nil {
				return nil, err
			}
		}

		// Accumulate data.
		accumulator = append(accumulator, cursor)
		count += batch.Count()

		// We have an over-large value. Flush immediately and let
		// the caller decide if it's going to open a transaction or
		// send as-is.
		if cursor.Fragment {
			ret.cursors = accumulator
			ret.fragment = true
			return ret, nil
		}

		// Flush if we've hit the target size.
		if count >= c.Config.FlushSize {
			ret.cursors = accumulator
			return ret, nil
		}

		// We can continue to accumulate data in subsequent passes.
		// This will start a timer on the second pass to ensure that
		// we flush within a reasonable time period.
		goto top

	case <-timerC:
		return &step{cursors: accumulator}, nil

	case <-ctx.Stopping():
		// Just exit if being shut down.
		return nil, nil
	}
}
