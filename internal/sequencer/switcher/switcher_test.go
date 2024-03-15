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
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/sequencer"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/seqtest"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/switcher"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/all"
	"github.com/cockroachdb/cdc-sink/internal/util/notify"
)

func TestSwitcher(t *testing.T) {
	seqtest.CheckSequencer(t,
		func(t *testing.T, fixture *all.Fixture, seqFixture *seqtest.Fixture) sequencer.Sequencer {
			ctx := fixture.Context
			mode := notify.VarOf(switcher.ModeBestEffort)
			fixture.Context.Go(func() error {
				for {
					select {
					case <-time.After(100 * time.Millisecond):
						_, _, _ = mode.Update(func(mode switcher.Mode) (switcher.Mode, error) {
							switch mode {
							case switcher.ModeBestEffort:
								return switcher.ModeShingle, nil
							case switcher.ModeShingle:
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
