// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package resolve

import (
	"github.com/cockroachdb/cdc-sink/internal/util/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	resolveFlushErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "resolve_flush_errors_total",
		Help: "the number of flush cycles that ended in an error",
	}, metrics.SchemaLabels)
	resolveFlushSuccess = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "resolve_flush_success_total",
		Help: "the number of successfully-executed flush cycles",
	}, metrics.SchemaLabels)
	resolveLastChecked = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "resolve_last_check_secondns",
		Help: "the wall time at which the last check for resolved timestamps was made",
	}, metrics.SchemaLabels)
	resolveLastHLC = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "resolve_resolved_hlc_seconds",
		Help: "the source-cluster resolved timestamp that has been processed",
	}, metrics.SchemaLabels)
	resolveLastSuccess = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "resolve_flush_success_seconds",
		Help: "the wall time at which the last resolve attempt successfully completed",
	}, metrics.SchemaLabels)
)
