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

package lockset

import (
	"fmt"
	"sync"

	"github.com/pkg/errors"
)

// A Queue implements an in-order admission queue for arbitrary values
// associated with a set of potentially-overlapping keys.
//
// A Queue is internally synchronized and is safe for concurrent use. A
// Queue should not be copied after it has been created.
type Queue[K comparable, V comparable] struct {
	mu struct {
		sync.RWMutex

		// These waiters are used to maintain a global ordering of
		// waiters to implement [RetryAtHead].
		head *queueEntry[K, V]
		tail *queueEntry[K, V]

		// Deadlocks between waiters are avoided since the relative
		// order of enqueued waiters is maintained. That is, if
		// Schedule() is called with W1 and then W2, the first waiter
		// will be ahead of the second in all key queues that they have
		// in common. Furthermore, first waiter is guaranteed to be
		// executed, since it will be at the head of all its key queues.
		queues map[K][]*queueEntry[K, V]
		ref    map[V]*queueEntry[K, V]
	}
}

// NewQueue constructs a [Queue].
func NewQueue[K comparable, V comparable]() *Queue[K, V] {
	q := &Queue[K, V]{}
	q.mu.queues = make(map[K][]*queueEntry[K, V])
	q.mu.ref = make(map[V]*queueEntry[K, V])
	return q
}

// Dequeue removes the value from the queue and returns any
// newly-unblocked values. The bool return value indicates whether the
// value was in the queue.
func (q *Queue[K, V]) Dequeue(val V) ([]V, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	w := q.mu.ref[val]
	// Not in the queue, so a no-op. Let the caller detemine if this is
	// an incorrect use-case or not.
	if w == nil {
		return nil, false
	}
	delete(q.mu.ref, val)

	var ret []V

	// Remove the waiter from each key's queue.
	for _, k := range w.keys {
		entries := q.mu.queues[k]

		// Search for the waiter in the queue. It's always going to
		// be the first element in the slice, except in the
		// cancellation case.
		var idx int
		for idx = range entries {
			if entries[idx] == w {
				break
			}
		}

		if idx == len(entries) {
			panic(fmt.Sprintf("waiter not found in queue: %d", idx))
		}

		// If the waiter was the first in the queue (likely),
		// promote the next waiter, possibly making it eligible to
		// be run.
		if idx == 0 {
			entries = entries[1:]
			if len(entries) == 0 {
				// The waiter was the only element of the queue, so
				// we'll just delete the slice from the map.
				delete(q.mu.queues, k)
				continue
			}

			// Promote the next waiter. If the waiter is now at the
			// head of its queues, it can be started.
			head := entries[0]
			head.headCount++
			if head.headCount == len(head.keys) {
				ret = append(ret, head.elt)
			} else if head.headCount > len(head.keys) {
				panic("over counted")
			}
		} else {
			// The (canceled) waiter was in the middle of the queue,
			// just remove it from the slice.
			entries = append(entries[:idx], entries[idx+1:]...)
		}

		// Put the shortened queue back in the map.
		q.mu.queues[k] = entries
	}

	// Make eligible for cleanup and remove key references.
	w.invalidate()

	// Clean up the global queue.
	head := q.mu.head
	for head != nil {
		if head.valid {
			break
		}
		head = head.next
	}
	q.mu.head = head
	if q.mu.head == nil {
		q.mu.tail = nil
	}

	return ret, true
}

// Enqueue returns true if the value is at the head of its key queues.
// It is an error to enqueue a value if it is already enqueued.
func (q *Queue[K, V]) Enqueue(keys []K, val V) (atHead bool, err error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, dup := q.mu.ref[val]; dup {
		return false, errors.Errorf("the value %v is already enqueued", val)
	}

	e := &queueEntry[K, V]{
		elt:   val,
		keys:  dedup(keys),
		valid: true,
	}
	q.mu.ref[val] = e

	// Insert the waiter into the global queue.
	if q.mu.tail == nil {
		q.mu.head = e
	} else {
		q.mu.tail.next = e
	}
	q.mu.tail = e

	// Add the waiter to each key queue. If it's the only waiter for
	// that key, also increment its headCount.
	for _, k := range e.keys {
		entries := q.mu.queues[k]
		entries = append(entries, e)
		q.mu.queues[k] = entries
		if len(entries) == 1 {
			e.headCount++
		}
	}

	// This will also be satisfied if the waiter has an empty key set.
	return e.headCount == len(e.keys), nil
}

// IsEmpty returns true if there are no elements in the queue.
func (q *Queue[K, V]) IsEmpty() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.mu.head == nil
}

// IsHead returns true if the value is at the head of the global queue.
func (q *Queue[K, V]) IsHead(val V) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	head := q.mu.head
	return head != nil && head.elt == val
}

// IsQueuedKey returns true if the key is present in the queue.
func (q *Queue[K, V]) IsQueuedKey(key K) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return len(q.mu.queues[key]) > 0
}

// IsQueuedValue returns true if the value is present in the queue.
func (q *Queue[K, V]) IsQueuedValue(val V) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	_, ok := q.mu.ref[val]
	return ok
}

// IsTail returns true if the value is at the tail of the global queue.
func (q *Queue[K, V]) IsTail(val V) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	tail := q.mu.tail
	return tail != nil && tail.elt == val
}

// PeekHead returns the value at the head of the global queue. It
// returns false if the queue is empty.
func (q *Queue[K, V]) PeekHead() (V, bool) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	h := q.mu.head
	if h == nil {
		return *new(V), false
	}
	return h.elt, true
}

// PeekTail returns the value at the head of the global queue. It
// returns false if the queue is empty.
func (q *Queue[K, V]) PeekTail() (V, bool) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	t := q.mu.tail
	if t == nil {
		return *new(V), false
	}
	return t.elt, true
}

type queueEntry[K any, V any] struct {
	headCount int
	elt       V
	keys      []K
	next      *queueEntry[K, V]
	valid     bool
}

func (q *queueEntry[K, V]) invalidate() {
	q.elt = *new(V)
	q.keys = nil
	q.valid = false
}

// Make a copy of the key slice and deduplicate it.
func dedup[K comparable](keys []K) []K {
	keys = append([]K(nil), keys...)
	seen := make(map[K]struct{}, len(keys))
	idx := 0
	for _, key := range keys {
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}

		keys[idx] = key
		idx++
	}
	keys = keys[:idx]
	return keys
}
