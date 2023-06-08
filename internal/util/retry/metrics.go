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
