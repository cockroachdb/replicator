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

package pglogical

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	dialFailureCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pglogical_dial_failure_total",
		Help: "the number of times we failed to create a replication connection",
	})
	dialSuccessCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pglogical_dial_success_total",
		Help: "the number of times we successfully dialed a replication connection",
	})
	skippedEmptyTransactions = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pglogical_skipped_empty_transactions",
		Help: "the number of times we skipped an empty transaction",
	})
)
