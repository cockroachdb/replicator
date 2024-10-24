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

package stage

import (
	"time"

	"github.com/spf13/pflag"
)

const (
	defaultMarkAppliedBatchSize = 100_000
	defaultSanityCheckPeriod    = 10 * time.Minute
	defaultSanityCheckWindow    = time.Hour
	defaultUnappliedPeriod      = time.Minute
)

// Config sets tuneables when interacting with the staging tables.
type Config struct {
	MarkAppliedLimit  int           // Maximum batch size for the MarkApplied method.
	SanityCheckPeriod time.Duration // If positive, refresh [stageConsistencyErrors].
	SanityCheckWindow time.Duration // If positive, limit time range of records.
	UnappliedPeriod   time.Duration // If positive, report number of unapplied mutations.
}

// Bind adds configuration flags to the set.
func (c *Config) Bind(f *pflag.FlagSet) {
	f.IntVar(&c.MarkAppliedLimit, "stageMarkAppliedLimit", defaultMarkAppliedBatchSize,
		"limit the number of mutations to be marked applied in a single statement")
	f.DurationVar(&c.SanityCheckPeriod, "stageSanityCheckPeriod", defaultSanityCheckPeriod,
		"how often to validate staging table apply order (-1 to disable)")
	f.DurationVar(&c.SanityCheckWindow, "stageSanityCheckWindow", defaultSanityCheckWindow,
		"how far back to look when validating staging table apply order")
	f.DurationVar(&c.UnappliedPeriod, "stageUnappliedPeriod", defaultUnappliedPeriod,
		"how often to report the number of unapplied mutations in staging tables (-1 to disable)")
}

// Preflight ensures the Config is in a known-good state.
func (c *Config) Preflight() error {
	if c.MarkAppliedLimit == 0 {
		c.MarkAppliedLimit = defaultMarkAppliedBatchSize
	}
	if c.SanityCheckPeriod == 0 {
		c.SanityCheckPeriod = defaultSanityCheckPeriod
	}
	if c.SanityCheckWindow == 0 {
		c.SanityCheckWindow = defaultSanityCheckWindow
	}
	if c.UnappliedPeriod == 0 {
		c.UnappliedPeriod = defaultUnappliedPeriod
	}
	return nil
}
