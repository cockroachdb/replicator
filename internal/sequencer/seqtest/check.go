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
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
	"testing/fstest"
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
	checkIdempotent
	checkPartitioned
	checkChaos

	checkMax           // Sentinel
	checkMin CheckFlag = 0
)

// CheckFlags returns all check combinations.
func CheckFlags() []CheckFlag {
	var ret []CheckFlag
	for i := checkMin; i < checkMax; i++ {
		if i.Partitioned() && i.Stage() {
			// Not a useful combination since remapping would have to
			// occur before writing data to staging.
			continue
		}
		ret = append(ret, i)
	}
	return ret
}

// Chaos returns true if the chaos shims will be installed.
func (f CheckFlag) Chaos() bool { return f&checkChaos == checkChaos }

// Idempotent returns true if the source should have idempotent replay.
func (f CheckFlag) Idempotent() bool { return f&checkIdempotent == checkIdempotent }

// Partitioned returns true if the source should have table names which
// do not exist in the target schema.
func (f CheckFlag) Partitioned() bool { return f&checkPartitioned == checkPartitioned }

// Stage returns true if mutations should be pre-staged before starting
// the sequencer.
func (f CheckFlag) Stage() bool { return f&checkStage == checkStage }

func (f CheckFlag) String() string {
	var sb strings.Builder
	if f.Stage() {
		sb.WriteString("stage")
	} else {
		sb.WriteString("direct")
	}
	if f.Idempotent() {
		sb.WriteString("-idempotent")
	} else {
		sb.WriteString("-non-idempotent")
	}
	if f.Partitioned() {
		sb.WriteString("-partitioned")
	}
	if f.Chaos() {
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
	const transactions = 1_000
	check := func(t *testing.T, flags CheckFlag) {
		if workloadCfg.DisableStaging && flags.Stage() {
			t.Log("staging disabled by WorkloadConfig")
			return
		}
		r := require.New(t)

		fixture, err := all.NewFixture(t)
		r.NoError(err)
		ctx := fixture.Context

		// Create sequencer test fixture.
		cfg := &sequencer.Config{
			FlushSize:        transactions/10 + 1,
			IdempotentSource: flags.Idempotent(),
			Parallelism:      8,
			QuiescentPeriod:  100 * time.Millisecond,
			ScanSize:         transactions/5 + 1,
			TimestampLimit:   transactions/10 + 1,
		}
		r.NoError(cfg.Preflight())

		scriptConfig := &script.Config{}
		if flags.Partitioned() {
			// Install a userscript that will strip trailing underscores
			// from input table names. That is, foo_N --> foo.
			scriptConfig.MainPath = "/main.ts"
			scriptConfig.FS = fstest.MapFS{
				"main.ts": {
					Data: []byte(fmt.Sprintf(`
import api from "replicator@v1";
function trimUnder(doc, meta) {
  let dest = meta.table.substring(0, meta.table.lastIndexOf("_"));
  return { [dest]: [ doc ] };
}
api.configureSource("%[1]s", {
  dispatch: trimUnder,
  deletesTo: trimUnder,
});
`, fixture.TargetSchema.Raw())),
				},
			}
		}
		seqFixture, err := NewSequencerFixture(fixture, cfg, scriptConfig)
		r.NoError(err)

		seq := pre(t, fixture, seqFixture)
		if scriptConfig.MainPath != "" {
			seq, err = seqFixture.Script.Wrap(ctx, seq)
			r.NoError(err)
		}
		if flags.Chaos() {
			cfg.Chaos = 2
			seq, err = seqFixture.Chaos.Wrap(ctx, seq)
			r.NoError(err)
		}
		basic := &Check{
			Transactions: transactions,
			Idempotent:   flags.Idempotent(),
			Fixture:      fixture,
			Partitioned:  flags.Partitioned(),
			Sequencer:    seq,
		}
		if flags.Stage() {
			basic.Stage = 0.5
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
	// Populated by Check.
	Bounds notify.Var[hlc.Range]
	// Access to test services.
	Fixture *all.Fixture
	// Populated by Check.
	Generator *all.Workload
	// Populated by Check.
	Group *types.TableGroup
	// Suppress non-idempotent replay of previous data.
	Idempotent bool
	// Simulate a fan-in case, where data from multiple source tables is
	// aggregated into a single target table.
	Partitioned bool
	// The Sequencer under test.
	Sequencer sequencer.Sequencer
	// A percentage of data to write to staging instead of to the
	// sequencer's acceptor.
	Stage float32
	// The total number of transactions to apply.
	Transactions int
}

// Check generates data and verifies that it reaches the target tables.
// It will also ensure that the staging tables are in a correct state.
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
	for i := 0; i < c.Transactions; i++ {
		generator.GenerateInto(testData, hlc.New(int64(i+1), 0))
	}

	if c.Partitioned {
		// Shuffle the test data into multiple source tables.
		count := 0
		partitionedBatch := &types.MultiBatch{}
		r.NoError(testData.CopyInto(types.AccumulatorFunc(
			func(table ident.Table, mut types.Mutation) error {
				partition := ident.NewTable(
					table.Schema(),
					ident.New(fmt.Sprintf("%s_%d", table.Table().Raw(), count%10)))
				count++
				return partitionedBatch.Accumulate(partition, mut)
			})))
		testData = partitionedBatch

	retryPartitioned:
		if err := seqAcc.AcceptMultiBatch(ctx, testData, &types.AcceptOptions{}); err != nil {
			if errors.Is(err, chaos.ErrChaos) {
				goto retryPartitioned
			}
			r.NoError(err)
		}
	} else {
		// We're going to fragment the batch to simulate data being
		// received piecemeal by multiple instances of Replicator.
		fragments, err := Fragment(testData, c.Transactions/10)
		r.NoError(err)

		for _, fragment := range fragments {
			// We may write some fraction of the data directly to the
			// staging tables. This simulates having another Replicator
			// instance receiving data behind our backs.
			if c.Stage > 0 && rand.Float32() < c.Stage {
				for _, temp := range fragment.Data {
					r.NoError(temp.Data.Range(func(table ident.Table, batch *types.TableBatch) error {
						stager, err := c.Fixture.Stagers.Get(ctx, table)
						if err != nil {
							return err
						}
						return stager.Stage(ctx, c.Fixture.StagingPool, batch.Data)
					}))
				}
				continue
			}

		retryFragment:
			if err := seqAcc.AcceptMultiBatch(ctx, fragment, &types.AcceptOptions{}); err != nil {
				if errors.Is(err, chaos.ErrChaos) {
					goto retryFragment
				}
				r.NoError(err)
			}
		}

		// We're also going to send a subset of stale data to simulate
		// non-idempotent replay from a changefeed.
		if !c.Idempotent {
			for idx, temporal := range testData.Data {
				if idx%10 != 1 {
					continue
				}
			retryRedeliver:
				if err := seqAcc.AcceptTemporalBatch(
					ctx, temporal, &types.AcceptOptions{},
				); err != nil {
					if errors.Is(err, chaos.ErrChaos) {
						goto retryRedeliver
					}
					r.NoError(err)
				}
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

	for _, table := range []ident.Table{generator.Parent.Name(), generator.Child.Name()} {
		stager, err := c.Fixture.Stagers.Get(ctx, table)
		r.NoError(err)
		ct, err := stager.CheckConsistency(ctx,
			c.Fixture.StagingPool, nil /* all records */, false /* current-time read */)
		r.NoError(err, table)
		r.Zero(ct, table)
	}
}
