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
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/script"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/sequencer/chaos"
	"github.com/cockroachdb/replicator/internal/sinktest/all"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// CheckFlag describes the range of configurations to be tested. See
// [CheckFlags].
type CheckFlag int

const (
	checkStage CheckFlag = 1 << iota
	checkChaos

	checkMax           // Sentinel
	checkMin CheckFlag = 0
)

// CheckFlags returns all check combinations.
func CheckFlags() []CheckFlag {
	var ret []CheckFlag
	for i := checkMin; i < checkMax; i++ {
		ret = append(ret, i)
	}
	return ret
}

func (f CheckFlag) String() string {
	var sb strings.Builder
	if f&checkStage == checkStage {
		sb.WriteString("stage")
	} else {
		sb.WriteString("direct")
	}
	if f&checkChaos == checkChaos {
		sb.WriteString("-chaos")
	}
	return sb.String()
}

// CheckSequencer implements a general-purpose smoke test of a
// [sequencer.Sequencer] implementation. The sequencer must
// support foreign-key relationships. The post-hook may be nil.
func CheckSequencer(
	t *testing.T,
	workloadCfg *all.WorkloadConfig,
	pre func(t *testing.T, fixture *all.Fixture, seqFixture *Fixture) sequencer.Sequencer,
	post func(t *testing.T, check *Check),
) {
	const batches = 1_000
	check := func(t *testing.T, flags CheckFlag) {
		if workloadCfg.DisablePreStaging && flags&checkStage == checkStage {
			t.Log("staging disabled by WorkloadConfig")
			return
		}
		r := require.New(t)

		fixture, err := all.NewFixture(t)
		r.NoError(err)
		ctx := fixture.Context

		// Create sequencer test fixture.
		cfg := &sequencer.Config{
			FlushSize:       batches/10 + 1,
			Parallelism:     8,
			QuiescentPeriod: 100 * time.Millisecond,
			ScanSize:        batches/5 + 1,
			TimestampLimit:  batches/10 + 1,
		}
		r.NoError(cfg.Preflight())

		seqFixture, err := NewSequencerFixture(fixture, cfg, &script.Config{})
		r.NoError(err)

		seq := pre(t, fixture, seqFixture)
		if flags&checkChaos == checkChaos {
			cfg.Chaos = 0.1
			seq, err = seqFixture.Chaos.Wrap(ctx, seq)
			r.NoError(err)
		}
		basic := &Check{
			Batches:   batches,
			Fixture:   fixture,
			Sequencer: seq,
			Stage:     flags&checkStage == checkStage,
		}
		basic.Check(ctx, t, workloadCfg)
		if post != nil {
			post(t, basic)
		}
	}

	for _, flag := range CheckFlags() {
		t.Run(flag.String(), func(t *testing.T) {
			check(t, flag)
		})
	}
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
	Generator *all.Workload
	// Populated by Check.
	Group *types.TableGroup
	// The Sequencer under test.
	Sequencer sequencer.Sequencer
	// If true, generated data will be loaded into staging first. This
	// is used to validate the sweeping behavior of a sequencer.
	Stage bool
}

// Check generates data and verifies that it reaches the target tables.
func (c *Check) Check(ctx *stopper.Context, t testing.TB, cfg *all.WorkloadConfig) {
	r := require.New(t)

	generator, group, err := c.Fixture.NewWorkload(ctx, cfg)
	r.NoError(err)
	c.Group = group

	startOpts := &sequencer.StartOptions{
		Bounds:   &c.Bounds,
		Delegate: types.OrderedAcceptorFrom(c.Fixture.ApplyAcceptor, c.Fixture.Watchers),
		Group:    group,
	}
	seqAcc, stats, err := c.Sequencer.Start(ctx, startOpts)
	r.NoError(err)
	c.Acceptor = seqAcc

	now := time.Now()
	testData := &types.MultiBatch{}
	for i := 0; i < c.Batches; i++ {
		generator.GenerateInto(testData, hlc.New(int64(i+1), 0))
	}

	if c.Stage {
		// Apply data to the staging table, to test sweeping behavior.
		for _, temporal := range testData.Data {
			r.NoError(temporal.Data.Range(func(table ident.Table, batch *types.TableBatch) error {
				stager, err := c.Fixture.Stagers.Get(ctx, table)
				if err != nil {
					return err
				}
				return stager.Stage(ctx, c.Fixture.StagingPool, batch.Data)
			}))
		}
	} else {
		// We're going to fragment the batch to simulate data being
		// received piecemeal by multiple instances of Replicator. We
		// ensure that the child fragments must be processed before the
		// parent fragments to ensure FK relationships are correctly
		// implemented.
		fragments, err := Fragment(testData)
		r.NoError(err)
		r.Len(fragments, 2)
		if _, isChild := fragments[1].Data[0].Data.Get(generator.Child.Name()); isChild {
			fragments[0], fragments[1] = fragments[1], fragments[0]
		}

		for _, fragment := range fragments {
		retryMulti:
			if err := seqAcc.AcceptMultiBatch(ctx, fragment, &types.AcceptOptions{}); err != nil {
				if errors.Is(err, chaos.ErrChaos) {
					goto retryMulti
				}
				r.NoError(err)
			}
		}

		// We're also going to send a subset of stale data to simulate
		// unrepeatable replay from a changefeed.
		for idx, temporal := range testData.Data {
			continue // XXX
			if idx%10 != 1 {
				continue
			}
		retryTemporal:
			if err := seqAcc.AcceptTemporalBatch(ctx, temporal, &types.AcceptOptions{}); err != nil {
				if errors.Is(err, chaos.ErrChaos) {
					goto retryTemporal
				}
				r.NoError(err)
			}
		}
	}
	log.Infof("accepted data in %s", time.Since(now))

	// Set desired range.
	c.Bounds.Set(generator.Range())
	r.NoError(err)

	// Wait to catch up.
	now = time.Now()
	r.NoError(generator.WaitForCatchUp(ctx, stats))
	log.Infof("caught up in an additional %s", time.Since(now))
	generator.CheckConsistent(ctx, t)

}
