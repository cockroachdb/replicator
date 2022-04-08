// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logical

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	commitSuccessCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "logical_commit_success_total",
		Help: "the number transactions from the source database that were applied",
	})
	commitFailureCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "logical_commit_failure_total",
		Help: "the number transactions from the source database that failed to apply",
	})
	commitOffset = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "logical_last_commit_offset_bytes",
		Help: "the offset that we are reporting to the source database",
	})
	commitTime = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "logical_last_commit_seconds",
		Help: "the original time of the most recently applied commit from the source database",
	})
)
