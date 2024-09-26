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
	"slices"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/field-eng-powertools/stopvar"
	"github.com/cockroachdb/replicator/internal/script"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/sinktest/all"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// CheckFlag describes the range of configurations to be tested. See
// [CheckFlags].
type CheckFlag int

const (
	checkIdempotent CheckFlag = 1 << iota
	checkPartitioned
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

// Chaos returns true if the chaos shims will be installed.
func (f CheckFlag) Chaos() bool { return f&checkChaos == checkChaos }

// Idempotent returns true if the source should have idempotent replay.
func (f CheckFlag) Idempotent() bool { return f&checkIdempotent == checkIdempotent }

// Partitioned returns true if the source should have table names which
// do not exist in the target schema.
func (f CheckFlag) Partitioned() bool { return f&checkPartitioned == checkPartitioned }

func (f CheckFlag) String() string {
	var sb strings.Builder
	sb.WriteString("check")
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
		if workloadCfg.DisableRedelivery && !flags.Idempotent() {
			t.Log("must be idempotent")
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
			Fixture:      fixture,
			Fragment:     !workloadCfg.DisableFragment,
			Idempotent:   flags.Idempotent(),
			Partitioned:  flags.Partitioned(),
			Sequencer:    seq,
			Transactions: transactions,
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
	// Populated by Check.
	Bounds notify.Var[hlc.Range]
	// Access to test services.
	Fixture *all.Fixture
	// Break the generated data up as though it's being received by
	// multiple instances of Replicator.
	Fragment bool
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

	testData := &types.MultiBatch{}
	for i := 0; i < c.Transactions; i++ {
		generator.GenerateInto(testData, hlc.New(int64(i+1), 0))
	}

	// Shuffle the test data into multiple source tables.
	if c.Partitioned {
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
	}

	// We're going to fragment the batch to simulate data being received
	// piecemeal by multiple instances of Replicator.
	if c.Fragment {
		fragments, err := Fragment(testData, c.Transactions/10)
		r.NoError(err)

		testData = &types.MultiBatch{
			ByTime: make(map[hlc.Time]*types.TemporalBatch, c.Transactions),
		}
		for _, fragment := range fragments {
			testData.Data = append(testData.Data, fragment.Data...)
		}
		slices.SortFunc(testData.Data, func(a, b *types.TemporalBatch) int {
			return hlc.Compare(a.Time, b.Time)
		})
		for _, temp := range testData.Data {
			testData.ByTime[temp.Time] = temp
		}
	}

	// We're also going to send a subset of stale data to simulate
	// non-idempotent replay from a changefeed.
	if !c.Idempotent {
		for idx, temporal := range testData.Data {
			if idx%10 != 1 {
				continue
			}

			testData.Data = append(testData.Data, temporal.Copy())
		}
	}

	sink := &dummyReader{
		batch:      testData,
		progressTo: generator.Range(),
	}
	sink.terminal.Set(make(map[*types.TemporalBatch]error, c.Transactions))

	startOpts := &sequencer.StartOptions{
		BatchReader: sink,
		Bounds:      &c.Bounds,
		Delegate:    types.OrderedAcceptorFrom(c.Fixture.ApplyAcceptor, c.Fixture.Watchers),
		Group:       group,
		Terminal: func(_ *stopper.Context, batch *types.TemporalBatch, err error) error {
			_, _, _ = sink.terminal.Update(func(m map[*types.TemporalBatch]error) (map[*types.TemporalBatch]error, error) {
				m[batch] = err
				return m, nil
			})
			return nil
		},
	}

	now := time.Now()
	stats, err := c.Sequencer.Start(ctx, startOpts)
	r.NoError(err)

	// Wait for all data to have been read out of the sink at least once.
	r.NoError(stopvar.WaitForValue(ctx, true, &sink.empty))
	log.Infof("source data consumed in %s", time.Since(now))

	// Set desired range.
	c.Bounds.Set(generator.Range())
	r.NoError(err)

	// Wait to catch up.
	now = time.Now()
	r.NoError(generator.WaitForCatchUp(ctx, stats))
	log.Infof("caught up in an additional %s", time.Since(now))

	// Ensure all generated batches eventually reached a successful
	// terminal state.
	_, err = sink.terminal.Peek(func(m map[*types.TemporalBatch]error) error {
		r.Len(m, len(testData.Data))
		for _, err := range m {
			r.NoError(err)
		}
		return nil
	})
	r.NoError(err)

	// Verify target contents.
	generator.CheckConsistent(ctx, t)

	// Ensure staging tables make sense.
	for _, table := range []ident.Table{generator.Parent.Name(), generator.Child.Name()} {
		stager, err := c.Fixture.Stagers.Get(ctx, table)
		r.NoError(err)
		ct, err := stager.CheckConsistency(ctx,
			c.Fixture.StagingPool, nil /* all records */, false /* current-time read */)
		r.NoError(err, table)
		r.Zero(ct, table)
	}

}

// This is a trivial implementation of [types.BatchReader] over canned
// data.
type dummyReader struct {
	batch      *types.MultiBatch
	empty      notify.Var[bool]
	progressTo hlc.Range
	terminal   notify.Var[map[*types.TemporalBatch]error]
}

var _ types.BatchReader = (*dummyReader)(nil)

func (r *dummyReader) Read(ctx *stopper.Context) (<-chan *types.BatchCursor, error) {
	ch := make(chan *types.BatchCursor)
	ctx.Go(func(ctx *stopper.Context) error {
		defer close(ch)
		for _, temp := range r.batch.Data {
			var send bool
			_, _ = r.terminal.Peek(func(m map[*types.TemporalBatch]error) error {
				err, ok := m[temp]
				send = err != nil || !ok
				return nil
			})
			if !send {
				continue
			}
			cur := &types.BatchCursor{
				Batch:    temp,
				Progress: hlc.RangeIncluding(hlc.Zero(), temp.Time),
			}
			select {
			case ch <- cur:
			case <-ctx.Stopping():
				return nil
			}
		}
		// Send progress update.
		cur := &types.BatchCursor{Progress: r.progressTo}
		select {
		case ch <- cur:
		case <-ctx.Stopping():
			return nil
		}
		r.empty.Set(true)
		// Become idle until shutdown, which is how a real
		// implementation would behave when no more data is available.
		<-ctx.Stopping()
		return nil
	})
	return ch, nil
}
