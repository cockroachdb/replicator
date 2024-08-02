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

package base

import (
	golog "log"
	"time"

	"github.com/cockroachdb/replicator/internal/util/logfmt"
	log "github.com/sirupsen/logrus"
)

// Provide reasonable defaults for logrus when testing.
func init() {
	pw := log.WithField("golog", true).Writer()
	log.DeferExitHandler(func() { _ = pw.Close() })
	// logrus will provide timestamp info.
	golog.SetFlags(0)
	golog.SetOutput(pw)

	log.SetFormatter(logfmt.Wrap(&log.TextFormatter{
		PadLevelText:    true,
		TimestampFormat: time.Stamp,
	}))
}
