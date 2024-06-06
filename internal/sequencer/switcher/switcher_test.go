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

package switcher_test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/sequencer/seqtest"
	"github.com/cockroachdb/replicator/internal/sequencer/switcher"
	"github.com/cockroachdb/replicator/internal/sinktest/all"
	"github.com/cockroachdb/replicator/internal/util/hlc"
)

func TestSwitcher(t *testing.T) {
	seqtest.CheckSequencer(t, &all.WorkloadConfig{},
		func(t *testing.T, fixture *all.Fixture, seqFixture *seqtest.Fixture) sequencer.Sequencer {
			ctx := fixture.Context
			// Ensure we cove both startup cases in CI.
			var initial switcher.Mode
			if rand.Float32() < 0.5 {
				initial = switcher.ModeBestEffort
			} else {
				initial = switcher.ModeConsistent
			}
			mode := notify.VarOf(initial)
			seqFixture.BestEffort.SetTimeSource(hlc.Zero) // The test rig uses fake timestamps.
			ctx.Go(func(ctx *stopper.Context) error {
				for {
					select {
					case <-time.After(time.Second):
						_, _, _ = mode.Update(func(mode switcher.Mode) (switcher.Mode, error) {
							switch mode {
							case switcher.ModeBestEffort:
								return switcher.ModeConsistent, nil
							case switcher.ModeConsistent:
								return switcher.ModeBestEffort, nil
							default:
								panic(fmt.Sprintf("unexpected state %s", mode))
							}
						})
					case <-ctx.Stopping():
						return nil
					}
				}
			})
			return seqFixture.Switcher.WithMode(mode)
		},
		func(t *testing.T, check *seqtest.Check) {})
}
