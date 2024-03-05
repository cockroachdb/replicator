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

package scheduler

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	executedCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "sequencer_scheduler_executed_tasks_count",
		Help: "the number of scheduled tasks that were executed",
	})
	executingCount = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "sequencer_scheduler_executing_tasks_count",
		Help: "the number of scheduled tasks currently executing",
	})
)
