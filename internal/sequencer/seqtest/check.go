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

package seqtest

import (
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/sequencer"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/chaos"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/all"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/notify"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// CheckSequencer implements a general-purpose smoke test of a
// [sequencer.Sequencer] implementation. The sequencer must
// support foreign-key relationships. The post-hook may be nil.
func CheckSequencer(
	t *testing.T,
	pre func(t *testing.T, fixture *all.Fixture, seqFixture *Fixture) sequencer.Sequencer,
	post func(t *testing.T, check *Check),
) {
	const batches = 100
	check := func(t *testing.T, stage bool, addChaos bool) {
		r := require.New(t)

		fixture, err := all.NewFixture(t)
		r.NoError(err)
		ctx := fixture.Context

		// Create sequencer test fixture.
		cfg := &sequencer.Config{
			Parallelism:     8,
			QuiescentPeriod: 100 * time.Millisecond,
			TimestampLimit:  batches/10 + 1,
			SweepLimit:      batches/10 + 1,
		}

		seqFixture, err := NewSequencerFixture(fixture, cfg, &script.Config{})
		r.NoError(err)

		seq := pre(t, fixture, seqFixture)
		if addChaos {
			cfg.Chaos = 0.1
			seq, err = seqFixture.Chaos.Wrap(ctx, seq)
			r.NoError(err)
		}
		basic := &Check{
			Batches:   batches,
			Fixture:   fixture,
			Sequencer: seq,
			Stage:     stage,
		}
		basic.Check(ctx, t)
		if post != nil {
			post(t, basic)
		}
	}

	t.Run("direct", func(t *testing.T) {
		check(t, false, false)
	})
	t.Run("direct-chaos", func(t *testing.T) {
		check(t, false, true)
	})
	t.Run("staged", func(t *testing.T) {
		check(t, true, false)
	})
	t.Run("staged-chaos", func(t *testing.T) {
		check(t, true, true)
	})
}

// Check implements a reusable test over a parent/child table pair.
type Check struct {
	// The acceptor returned by [sequencer.Sequencer.Start].
	Acceptor types.MultiAcceptor
	// The total number of transactions to apply.
	Batches int
	// Populated by Check.
	Bounds notify.Var[hlc.Range]
	// Access to test services.
	Fixture *all.Fixture
	// Populated by Check.
	Generator *Generator
	// Populated by Check.
	Group *types.TableGroup
	// The Sequencer under test.
	Sequencer sequencer.Sequencer
	// If true, generated data will be loaded into staging first. This
	// is used to validate the sweeping behavior of a sequencer.
	Stage bool
}

// Check generates data and verifies that it reaches the target tables.
func (b *Check) Check(ctx *stopper.Context, t testing.TB) {
	r := require.New(t)

	generator, group, err := NewGenerator(ctx, b.Fixture)
	r.NoError(err)
	b.Group = group

	seqAcc, stats, err := b.Sequencer.Start(ctx, &sequencer.StartOptions{
		Bounds:   &b.Bounds,
		Delegate: types.OrderedAcceptorFrom(b.Fixture.ApplyAcceptor, b.Fixture.Watchers),
		Group:    group,
	})
	r.NoError(err)
	b.Acceptor = seqAcc

	now := time.Now()
	testData := &types.MultiBatch{}
	for i := 0; i < b.Batches; i++ {
		generator.GenerateInto(testData, hlc.New(int64(i+1), 0))
	}

	if b.Stage {
		// Apply data to the staging table, to test sweeping behavior.
		for _, temporal := range testData.Data {
			r.NoError(temporal.Data.Range(func(table ident.Table, batch *types.TableBatch) error {
				stager, err := b.Fixture.Stagers.Get(ctx, table)
				if err != nil {
					return err
				}
				return stager.Stage(ctx, b.Fixture.StagingPool, batch.Data)
			}))
		}
	} else {
		// We're going to fragment the batch to simulate data being
		// received piecemeal by multiple instances of cdc-sink. We
		// ensure that the child fragments must be processed before
		// the parent fragments.
		fragments, err := Fragment(testData)
		r.NoError(err)
		r.Len(fragments, 2)
		if _, isChild := fragments[1].Data[0].Data.Get(generator.Child.Name()); isChild {
			fragments[0], fragments[1] = fragments[1], fragments[0]
		}

	retry:
		for _, fragment := range fragments {
			if err := seqAcc.AcceptMultiBatch(ctx, fragment, &types.AcceptOptions{}); err != nil {
				if errors.Is(err, chaos.ErrChaos) {
					goto retry
				}
				r.NoError(err)
			}
		}
	}
	log.Infof("accepted data in %s", time.Since(now))

	// Set desired range.
	b.Bounds.Set(generator.Range())
	r.NoError(err)

	// Wait to catch up.
	now = time.Now()
	r.NoError(generator.WaitForCatchUp(ctx, stats))
	log.Infof("caught up in an additional %s", time.Since(now))
	generator.CheckConsistent(ctx, t)

}
