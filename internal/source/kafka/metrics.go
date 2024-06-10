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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// TODO (silvano) Provide a grafana dashboard for kafka connector.
// https://github.com/cockroachdb/replicator/issues/829
var (
	cacheSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_cache_size",
		Help: "the number of entries in the cache",
	}, []string{"topic", "partition"})
	delayDuration = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_delay_seconds",
		Help: "the delta between now and the latest resolved timestamp",
	}, []string{"topic", "partition"})
	duplicateMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_duplicate_count",
		Help: "the total of duplicate messages",
	}, []string{"topic", "partition"})
	evictDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kafka_cache_evictions_duration",
		Help:    "the length of time it took to evict expired entries from the cache",
		Buckets: prometheus.ExponentialBucketsRange(1, 1e6, 20),
	}, []string{"topic", "partition"})
	oldMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_old_messages_count",
		Help: "the total of messages older than the last resolved timestamp",
	}, []string{"topic", "partition"})
	purgeDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kafka_cache_purges_duration",
		Help:    "the length of time it took to purge the cache",
		Buckets: prometheus.ExponentialBucketsRange(1, 1e6, 20),
	}, []string{"topic", "partition"})
	seekMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_seeks_count",
		Help: "the total of messages read seeking a minimum resolved timestamp",
	}, []string{"topic", "partition"})
)
