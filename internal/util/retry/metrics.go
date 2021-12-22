// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package retry

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	abortedCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "retry_aborted_total",
		Help: "the number of times an unretryable pgwire error code was seen",
	}, []string{"code"})
	actionsCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "retry_actions_total",
		Help: "the total number of retryable actions attempted",
	})
	retryCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "retry_retried_total",
		Help: "the number of times a retryable pgwire error was seen",
	}, []string{"code"})
)
