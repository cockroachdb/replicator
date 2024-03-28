// Copyright 2023 The Cockroach Authors
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

package stmtcache

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	stmtCacheDrops = promauto.NewCounter(prometheus.CounterOpts{
		Name: "stmtcache_drops_total",
		Help: "the number of prepared statements that could not be cleanly released",
	})
	stmtCacheHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "stmtcache_hits_total",
		Help: "the number of prepared-statement cache hits",
	})
	stmtCacheMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "stmtcache_misses_total",
		Help: "the number of prepared-statement cache misses",
	})
	stmtCacheReleases = promauto.NewCounter(prometheus.CounterOpts{
		Name: "stmtcache_releases_total",
		Help: "the number of prepared statements released by the cache",
	})
)
