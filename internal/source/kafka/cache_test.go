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

package kafka

import (
	"fmt"
	"testing"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func BenchmarkEvict(b *testing.B) {
	r := require.New(b)
	impl, err := lru.New[string, hlc.Time](b.N * 2)
	r.NoError(err)
	cache := &cache{
		impl: impl,
	}
	for i := 0; i < b.N; i++ {
		cache.impl.Add(fmt.Sprintf("%d", i), hlc.New(int64(i), 0))
	}
	cache.setLow(hlc.New(int64(b.N), 0))
	cache.maybeEvict()
}

func BenchmarkPurge(b *testing.B) {
	r := require.New(b)
	impl, err := lru.New[string, hlc.Time](b.N * 2)
	r.NoError(err)
	cache := &cache{
		impl: impl,
	}
	for i := 0; i < b.N; i++ {
		cache.impl.Add(fmt.Sprintf("%d", i), hlc.New(int64(i), 0))
	}
	cache.impl.Purge()
}

func TestCache(t *testing.T) {
	num := 1000
	r := require.New(t)
	a := assert.New(t)
	impl, err := lru.New[string, hlc.Time](num * 2)
	r.NoError(err)
	cache := &cache{
		impl: impl,
	}
	// Adding (0,0) to (999,0) to the cache
	for i := 0; i < num; i++ {
		msg := &sarama.ConsumerMessage{
			Key: []byte(fmt.Sprintf("%d", i)),
		}
		a.True(cache.inNewer(msg, hlc.New(int64(i), 0)))
	}
	// Adding (1000,0) to (1999,0) to the cache
	for i := 0; i < num; i++ {
		msg := &sarama.ConsumerMessage{
			Key: []byte(fmt.Sprintf("%d", i)),
		}
		a.True(cache.inNewer(msg, hlc.New(int64(num+i), 0)))
	}
	// Replaying (0,0) to (999,0) to the cache
	for i := 0; i < num; i++ {
		msg := &sarama.ConsumerMessage{
			Key: []byte(fmt.Sprintf("%d", i)),
		}
		a.False(cache.inNewer(msg, hlc.New(int64(i), 0)))
	}
	// Mark resolved everything older than  (1500,0)
	cache.markResolved(hlc.New(int64(num+num/2), 0))
	// Evict older entries
	cache.maybeEvict()
	// We should have everything after and including (1500,0)
	a.Equal(500, cache.impl.Len())
	for _, timestamp := range cache.impl.Values() {
		compare := func() bool { return hlc.Compare(timestamp, hlc.New(int64(num+num/2), 0)) >= 0 }
		a.Condition(compare)
	}
	// Mark resolved everything older than  (2001,0)
	cache.markResolved(hlc.New(int64(num*2+1), 0))
	// Cache should be empty
	a.Equal(0, cache.impl.Len())
}
