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

package besteffort_test

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/sequencer/seqtest"
	"github.com/cockroachdb/replicator/internal/sinktest/all"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/stretchr/testify/require"
)

func TestBestEffort(t *testing.T) {
	tcs := []*all.WorkloadConfig{
		{},                // Verify grouped-table behaviors.
		{DisableFK: true}, // Verify single-table behaviors.
	}

	for idx, cfg := range tcs {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			seqtest.CheckSequencer(t, cfg,
				func(t *testing.T, fixture *all.Fixture, seqFixture *seqtest.Fixture) sequencer.Sequencer {
					// We only want BestEffort to do work when told; the test
					// rig uses a counter to generate timestamps.
					seqFixture.BestEffort.SetTimeSource(hlc.Zero)
					next, err := seqFixture.BestEffort.Wrap(fixture.Context, seqFixture.Core)
					require.NoError(t, err)
					return next
				},
				func(t *testing.T, check *seqtest.Check) {})
		})
	}
}
