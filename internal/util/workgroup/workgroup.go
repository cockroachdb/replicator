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

// Package workgroup contains a concurrency-control utility.
package workgroup

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/pkg/errors"
)

const idleNanos = int64(time.Second)

type callback func(context.Context)

// Group is a basic concurrency-control mechanism that has a
// variable-sized pool of worker goroutines executing callbacks from a
// queue. Unlike an [errgroup.Group], this types does not allow awaiting
// on outcomes of callbacks.
//
// A Group is safe to call from multiple goroutines. A Group should not
// be copied once created.
type Group struct {
	ctx        context.Context
	handoff    chan callback // Synchronous channel for immediate dispatch.
	maxWorkers int
	queue      chan callback // Buffered channel for work backlog.

	mu struct {
		sync.Mutex
		numWorkers int
	}
}

// WithSize returns a [Group] that will execute with up to the
// requested number of goroutines and queue up to the requested number
// of work elements.
func WithSize(ctx context.Context, maxWorkers int, maxQueueDepth int) *Group {
	return &Group{
		ctx:        ctx,
		handoff:    make(chan callback),
		maxWorkers: maxWorkers,
		queue:      make(chan callback, maxQueueDepth),
	}
}

// Go executes the callback in a worker goroutine. If all workers have
// been created and the queue is full, an error will be returned.
func (g *Group) Go(fn func(ctx context.Context)) error {
	if err := g.ctx.Err(); err != nil {
		return err
	}

	// Synchronous handoff to a waiting worker.
	select {
	case g.handoff <- fn:
		return nil
	default:
	}

	// Warm-up case where we start a worker to handle the work unit.
	if g.maybeStart(fn) {
		return nil
	}

	select {
	case g.queue <- fn:
		// This represents an exceedingly unlikely case where all
		// workers simultaneously selected on their idle channel and
		// exited instead of consuming a work unit.
		g.maybeStart(nil)
		return nil
	default:
		return errors.Errorf("queue depth %d exceeded", cap(g.queue))
	}
}

// Len returns the number of queued work items.
func (g *Group) Len() int {
	return len(g.queue)
}

// maybeStart will return true if started a worker goroutine that is
// guaranteed to execute the callback.
func (g *Group) maybeStart(fn callback) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.mu.numWorkers >= g.maxWorkers {
		return false
	}
	g.mu.numWorkers++

	// If we start a worker, we want to know that the initiating
	// callback will be executed by the worker. This lends
	// predictability to the number of times Go can be called before
	// it will start returning errors.
	go g.worker(g.ctx, fn)
	return true
}

func (g *Group) worker(ctx context.Context, initial callback) {
	defer func() {
		g.mu.Lock()
		g.mu.numWorkers--
		g.mu.Unlock()

		// When a worker exits, we want to ensure that a replacement
		// worker will be available to pick up any leftover work that
		// this worker could have picked up.
		if len(g.queue) > 0 && ctx.Err() == nil {
			g.maybeStart(nil)
		}
	}()

	if initial != nil {
		initial(ctx)
		initial = nil
	}

	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		// Reset timer and smear timeout behaviors.
		if !timer.Stop() {
			<-timer.C
		}
		timer.Reset(time.Duration(idleNanos + rand.Int63n(idleNanos)))

		select {
		case next := <-g.handoff:
			// Execute the next work unit from a synchronous handoff.
			next(ctx)

		case next := <-g.queue:
			// Execute the next work unit out of the backlog.
			next(ctx)

		case <-timer.C:
			// If we've been idle for a while, shed goroutines.
			return

		case <-ctx.Done():
			// Time to shut down.
			return
		}
	}
}
