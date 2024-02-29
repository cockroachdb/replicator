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

package workgroup

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/util/notify"
	"github.com/stretchr/testify/require"
)

func TestWorkGroup(t *testing.T) {
	r := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	block := make(chan struct{})

	const workers = 16
	const depth = 32

	var calls notify.Var[int]
	wg := WithSize(ctx, workers, depth)

	// Ensure we can start the expected number of workers and add
	// additional elements to the queue.
	for i := 0; i < workers+depth; i++ {
		r.NoError(wg.Go(func(ctx context.Context) {
			select {
			case <-block:
				_, _, _ = calls.Update(func(old int) (int, error) {
					return old + 1, nil
				})
			case <-ctx.Done():
			}
		}))
	}

	// Ensure all workers were started.
	wg.mu.Lock()
	count := wg.mu.numWorkers
	wg.mu.Unlock()
	r.Equal(workers, count)

	// Ensure queue is full.
	r.Equal(depth, wg.Len())

	// Check we can't add any more work.
	r.ErrorContains(wg.Go(func(ctx context.Context) {}),
		fmt.Sprintf("queue depth %d exceeded", depth))

	// Allow workers to drain.
	close(block)

	// Wait for backlog to be processed.
	for {
		count, changed := calls.Get()
		if count == workers+depth {
			break
		}
		select {
		case <-changed:
		case <-ctx.Done():
			r.NoError(ctx.Err())
		}
	}

	// Test synchronous handoff to the now-warmed-up worker pool.
	for i := 0; i < workers; i++ {
		r.NoError(wg.Go(func(ctx context.Context) {
			select {
			case <-block:
				_, _, _ = calls.Update(func(old int) (int, error) {
					return old + 1, nil
				})
			case <-ctx.Done():
			}
		}))
	}

	// Wait for backlog to be processed.
	for {
		count, changed := calls.Get()
		if count == 2*workers+depth {
			break
		}
		select {
		case <-changed:
		case <-ctx.Done():
			r.NoError(ctx.Err())
		}
	}

	// Crash all workers.
	for i := 0; i < workers; i++ {
		r.NoError(wg.Go(func(ctx context.Context) {
			runtime.Goexit()
		}))
	}

	// Ensure we can recover from worker-pool failure.
	restored := make(chan struct{})
	r.NoError(wg.Go(func(ctx context.Context) {
		close(restored)
	}))

	select {
	case <-restored:
	// Success.
	case <-ctx.Done():
		r.NoError(ctx.Err())
	}

	// Verify cancellation behavior.
	cancel()
	r.ErrorIs(wg.Go(nil), context.Canceled)
}
