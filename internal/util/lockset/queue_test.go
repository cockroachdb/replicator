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
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQueueSmoke(t *testing.T) {
	r := require.New(t)

	q := NewQueue[int, string]()
	keys := []int{1}

	r.True(q.IsEmpty())
	r.False(q.IsHead("1a"))
	r.False(q.IsTail("1a"))
	r.False(q.IsQueuedValue("1a"))
	r.False(q.IsQueuedKey(keys[0]))

	r.True(q.Enqueue(keys, "1a"))
	r.False(q.IsEmpty())
	r.True(q.IsHead("1a"))
	r.True(q.IsTail("1a"))
	r.True(q.IsQueuedValue("1a"))
	r.True(q.IsQueuedKey(keys[0]))
	peek, ok := q.PeekHead()
	r.True(ok)
	r.Equal("1a", peek)
	peek, ok = q.PeekTail()
	r.True(ok)
	r.Equal("1a", peek)

	// Not allowed to double-enqueue
	_, err := q.Enqueue(keys, "1a")
	r.EqualError(err, "the value 1a is already enqueued")

	r.False(q.IsHead("1b"))
	r.False(q.IsTail("1b"))
	r.False(q.Enqueue(keys, "1b"))

	r.True(q.IsHead("1a"))
	r.False(q.IsTail("1a"))
	r.False(q.IsHead("1b"))
	r.True(q.IsTail("1b"))
	peek, ok = q.PeekHead()
	r.True(ok)
	r.Equal("1a", peek)
	peek, ok = q.PeekTail()
	r.True(ok)
	r.Equal("1b", peek)

	next, ok := q.Dequeue("1a")
	r.True(ok)
	r.Equal([]string{"1b"}, next)
	r.True(q.IsHead("1b"))

	peek, ok = q.PeekHead()
	r.True(ok)
	r.Equal("1b", peek)
	peek, ok = q.PeekTail()
	r.True(ok)
	r.Equal("1b", peek)

	// It's not an error to repeatedly dequeue.
	next, ok = q.Dequeue("1a")
	r.False(ok)
	r.Nil(next)

	next, ok = q.Dequeue("1b")
	r.True(ok)
	r.Nil(next)

	r.True(q.IsEmpty())

	peek, ok = q.PeekHead()
	r.False(ok)
	r.Equal("", peek)
	peek, ok = q.PeekTail()
	r.False(ok)
	r.Equal("", peek)
}

func TestQueueMultipleKeys(t *testing.T) {
	r := require.New(t)

	q := NewQueue[int, string]()

	r.True(q.Enqueue([]int{1, 2, 3, 4, 5}, "one"))
	r.False(q.Enqueue([]int{2, 3, 4, 5}, "two"))
	r.False(q.Enqueue([]int{3, 4, 5}, "three"))
	r.False(q.Enqueue([]int{4}, "four-only"))
	r.False(q.Enqueue([]int{5}, "five-only"))

	r.Nil(q.Dequeue("two"))

	next, ok := q.Dequeue("one")
	r.True(ok)
	r.Equal([]string{"three"}, next)

	next, ok = q.Dequeue("three")
	r.True(ok)
	r.Equal([]string{"four-only", "five-only"}, next)
}

func TestQueueManyWaiters(t *testing.T) {
	r := require.New(t)

	q := NewQueue[int, string]()

	for i := range 100 {
		atHead, err := q.Enqueue([]int{1, 2}, strconv.Itoa(i))
		r.NoError(err)
		if i == 0 {
			r.True(atHead)
		} else {
			r.False(atHead)
		}
	}

	for i := range 100 {
		next, ok := q.Dequeue(strconv.Itoa(i))
		r.True(ok)
		if i == 99 {
			r.Nil(next)
		} else {
			r.Equal([]string{strconv.Itoa(i + 1)}, next)
		}
	}
}
