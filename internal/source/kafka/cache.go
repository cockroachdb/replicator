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
	"strconv"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	lru "github.com/hashicorp/golang-lru/v2"
	log "github.com/sirupsen/logrus"
)

const (
	disableCache = 0
)

// cache keeps track of the timestamps for each key on a partition basis.
type cache struct {
	impl      *lru.Cache[string, hlc.Time]
	partition string
	size      int
	topic     string
	mu        struct {
		sync.RWMutex
		high hlc.Time // high watermark: the most recent timestamp we encountered.
		low  hlc.Time // low watermark corresponds to the latest resolved timestamp.
	}
}

// newCache creates a LRU cache to track timestamps for each primary key
// for the given topic and partition.
func newCache(ctx *stopper.Context, topic string, partition, size int) (*cache, error) {
	if size > disableCache {
		impl, err := lru.New[string, hlc.Time](size)
		if err != nil {
			return nil, err
		}
		cache := &cache{
			impl:      impl,
			partition: strconv.Itoa(partition),
			size:      size,
			topic:     topic,
		}
		// TODO (Silvano):  Is this strictly necessary? Can we just have the cache
		// clean up as needed. Maybe we can make it optional with a config flag.
		cache.backgroundEvict(ctx)
		return cache, nil
	}
	return nil, nil
}

// backgroundEvict periodically removes entries that are older than
// the low watermark.
func (c *cache) backgroundEvict(ctx *stopper.Context) {
	if c == nil {
		return
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	ctx.Go(func(ctx *stopper.Context) error {
		for {
			c.maybeEvict()
			select {
			case <-ctx.Stopping():
				log.Infof("stopping cache eviction for %s.%s", c.topic, c.partition)
				return nil
			case <-ticker.C:
			}
		}
	})
}

// getHigh return the high watermark timestamp
func (c *cache) getHigh() hlc.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mu.high
}

// getHigh return the low watermark timestamp
func (c *cache) getLow() hlc.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mu.low
}

// inNewer check if the timestamp for this message key is older than
// the latest timestamp we received for the same key.
// If it is newer it updates the entry in the cache.
func (c *cache) inNewer(msg *sarama.ConsumerMessage, timestamp hlc.Time) bool {
	if c == nil {
		return true
	}
	if hlc.Compare(timestamp, c.getHigh()) > 0 {
		c.setHigh(timestamp)
	}
	if hlc.Compare(timestamp, c.getLow()) < 0 {
		// A message came after the resolved timestamp.
		// This shouldn't happen, but we track it.
		oldMessagesCount.WithLabelValues(c.topic, c.partition).Inc()
		return false
	}
	key := fmt.Sprintf("%s-.-%d-.-%s", msg.Topic, msg.Partition, msg.Key)
	if previous, ok := c.impl.Get(string(key)); ok && hlc.Compare(timestamp, previous) < 0 {
		duplicateMessagesCount.WithLabelValues(c.topic, c.partition).Inc()
		return false
	}
	c.impl.Add(string(key), timestamp)
	return true
}

// markResolved indicates that any entries older that the given timestamp can be evicted.
// if there are no entries that have a newer timestamp, the cache is purged.
func (c *cache) markResolved(timestamp hlc.Time) {
	if c == nil {
		return
	}
	// if there is nothing newer we just wipe the cache.
	if hlc.Compare(timestamp, c.getHigh()) > 0 {
		start := time.Now()
		c.impl.Purge()
		dur := time.Since(start)
		purgeDuration.WithLabelValues(c.topic, c.partition).Observe(float64(dur.Microseconds()))
		c.setHigh(timestamp)
	}
	// Anything older that this timestamp can be cleaned.
	c.setLow(timestamp)
}

// maybeEvict evicts the entries that have a timestamp value older that the low watermark.
// It scans entries started from the oldest entry added to the cache. Given that the
// timestamp (values) in the cache are not necessarily in the same order of when they are
// added in the cache, the algorithm is best effort.
func (c *cache) maybeEvict() {
	count := 0
	start := time.Now()
	for {
		cacheSize.WithLabelValues(c.topic, c.partition).Set(float64(c.impl.Len()))
		if k, ts, ok := c.impl.GetOldest(); ok && hlc.Compare(ts, c.getLow()) < 0 {
			count++
			c.impl.Remove(k)
			continue
		}
		if count > 0 {
			dur := time.Since(start)
			evictDuration.WithLabelValues(c.topic, c.partition).Observe(float64(dur.Microseconds()))
		}
		return
	}
}

// setHigh updates the high watermark timestamp
func (c *cache) setHigh(timestamp hlc.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.high = timestamp
}

// setLow updates the low watermark timestamp
func (c *cache) setLow(timestamp hlc.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.low = timestamp
}
