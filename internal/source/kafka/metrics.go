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
	mutationsErrorCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_mutations_error_count",
		Help: "the total number of mutations that encountered an error during processing",
	}, []string{"topic", "partition"})
	mutationsReceivedCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_mutations_received_count",
		Help: "the total number of mutations received from the source",
	}, []string{"topic", "partition"})
	mutationsSuccessCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_mutations_success_count",
		Help: "the total number of mutations that were successfully processed",
	}, []string{"topic", "partition"})
	seekMessagesCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_seeks_count",
		Help: "the total of messages read seeking a minimum resolved timestamp",
	}, []string{"topic", "partition"})
)
