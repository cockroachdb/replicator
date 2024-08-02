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

package logfmt

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"
)

var (
	messageCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "log_message_count",
		Help: "the number of log messages emitted at the given level",
	}, []string{"level"})
)

func init() {
	// Ensure all levels are populated so we can graph a zero value.
	for _, level := range log.AllLevels {
		messageCount.WithLabelValues(level.String()).Add(0)
	}
}
