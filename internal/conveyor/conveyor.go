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

// Package conveyor delivers mutations to target.
package conveyor

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/sequencer/retire"
	"github.com/cockroachdb/replicator/internal/sequencer/switcher"
	"github.com/cockroachdb/replicator/internal/staging/checkpoint"
	"github.com/cockroachdb/replicator/internal/target/apply"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/notify"
	"github.com/cockroachdb/replicator/internal/util/stopper"
	"github.com/cockroachdb/replicator/internal/util/stopvar"
	log "github.com/sirupsen/logrus"
)

// Conveyors manages the plumbing necessary to deliver mutations to a
// target schema across multiple partitions. It is also responsible for
// mode-switching.
type Conveyors struct {
	cfg           *Config                 // Controls the mode of operations.
	checkpoints   *checkpoint.Checkpoints // Checkpoints factory.
	kind          string                  // Used by metrics.
	retire        *retire.Retire          // Removes old mutations.
	stopper       *stopper.Context        // Manages the lifetime of the goroutines.
	switcher      *switcher.Switcher      // Switches between mode of operations.
	tableAcceptor *apply.Acceptor         // Writes batches of mutations into target tables.
	watchers      types.Watchers          // Target schema access.

	mu struct {
		sync.RWMutex
		targets ident.SchemaMap[*Conveyor] // Conveyor cache
	}
}

// Get returns a conveyor for a specific schema.
func (c *Conveyors) Get(schema ident.Schema) (*Conveyor, error) {
	c.mu.RLock()
	found, ok := c.mu.targets.Get(schema)
	c.mu.RUnlock()
	if ok {
		return found, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check.
	if found, ok := c.mu.targets.Get(schema); ok {
		return found, nil
	}

	w, err := c.watchers.Get(schema)
	if err != nil {
		return nil, err
	}
	var tables []ident.Table
	_ = w.Get().Columns.Range(func(tbl ident.Table, _ []types.ColData) error {
		tables = append(tables, tbl)
		return nil
	})

	tableGroup := &types.TableGroup{
		Enclosing: schema,
		Name:      ident.New(schema.Raw()),
		Tables:    tables,
	}

	ret := &Conveyor{
		factory: c,
		target:  schema,
		watcher: w,
	}

	ret.checkpoint, err = c.checkpoints.Start(c.stopper, tableGroup, &ret.resolvingRange)
	if err != nil {
		return nil, err
	}

	// Set the mode before starting the switcher.
	ret.modeSelector(c.stopper)

	ret.acceptor, ret.stat, err = c.switcher.WithMode(&ret.mode).Start(
		c.stopper,
		&sequencer.StartOptions{
			Bounds:   &ret.resolvingRange,
			Delegate: types.OrderedAcceptorFrom(c.tableAcceptor, c.watchers),
			Group:    tableGroup,
		})
	if err != nil {
		return nil, err
	}

	// Add top-of-funnel reporting.
	labels := []string{c.kind, schema.Raw()}
	ret.acceptor = types.CountingAcceptor(ret.acceptor,
		mutationsErrorCount.WithLabelValues(labels...),
		mutationsReceivedCount.WithLabelValues(labels...),
		mutationsSuccessCount.WithLabelValues(labels...),
	)

	// Advance the stored resolved timestamps.
	ret.updateResolved(c.stopper)

	// Allow old staged mutations to be retired.
	c.retire.Start(c.stopper, tableGroup, &ret.resolvingRange)

	// Report timestamps and lag.
	ret.metrics(c.stopper)

	c.mu.targets.Put(schema, ret)
	return ret, nil
}

// WithKind returns a new Conveyors factory for the named kind.
func (c *Conveyors) WithKind(kind string) *Conveyors {
	res := c.clone()
	res.kind = kind
	return res
}

// Bootstrap existing schemas for recovery cases.
func (c *Conveyors) Bootstrap() error {
	schemas, err := c.checkpoints.ScanForTargetSchemas(c.stopper)
	if err != nil {
		return err
	}
	for _, sch := range schemas {
		_, err := c.Get(sch)
		if err != nil {
			return err
		}
	}
	return nil
}

// clone makes a shallow copy of the factory. The internal cache is not copied.
func (c *Conveyors) clone() *Conveyors {
	return &Conveyors{
		cfg:           c.cfg,
		checkpoints:   c.checkpoints,
		kind:          c.kind,
		retire:        c.retire,
		stopper:       c.stopper,
		switcher:      c.switcher,
		tableAcceptor: c.tableAcceptor,
		watchers:      c.watchers,
	}
}

// A Conveyor delivers mutations to a target, possibly asynchronously.
// It provides an abstraction over various delivery strategies and it
// manages checkpoints across multiple partitions for a table group.
type Conveyor struct {
	acceptor       types.MultiAcceptor         // Possibly-async writes to the target.
	checkpoint     *checkpoint.Group           // Persistence of checkpoint (fka. resolved) timestamps
	factory        *Conveyors                  // Factory that created this conveyor.
	mode           notify.Var[switcher.Mode]   // Switchable strategies.
	resolvingRange notify.Var[hlc.Range]       // Range of resolved timestamps to be processed.
	stat           *notify.Var[sequencer.Stat] // Processing status.
	target         ident.Schema                // Identify for logging.
	watcher        types.Watcher               // Schema info.
}

// AcceptMultiBatch transmits the batch. The options may be nil.
func (c *Conveyor) AcceptMultiBatch(
	ctx context.Context, batch *types.MultiBatch, options *types.AcceptOptions,
) error {
	return c.acceptor.AcceptMultiBatch(ctx, batch, options)
}

// Advance the checkpoint for all the named partitions.
func (c *Conveyor) Advance(ctx context.Context, partition ident.Ident, ts hlc.Time) error {
	return c.checkpoint.Advance(ctx, partition, ts)
}

// Ensure that a checkpoint exists for all named partitions.
func (c *Conveyor) Ensure(ctx context.Context, partitions []ident.Ident) error {
	return c.checkpoint.Ensure(ctx, partitions)
}

// Range returns the range of resolved timestamps to be processed.
func (c *Conveyor) Range() *notify.Var[hlc.Range] {
	return &c.resolvingRange
}

// Refresh is used for testing to refresh the checkpoint.
func (c *Conveyor) Refresh() {
	c.checkpoint.Refresh()
}

// TableGroup returns the TableGroup associated to this conveyor.
func (c *Conveyor) TableGroup() *types.TableGroup {
	return c.checkpoint.TableGroup()
}

// Watcher is used for testing to gain access to the underlying schema.
func (c *Conveyor) Watcher() types.Watcher {
	return c.watcher
}

func (c *Conveyor) metrics(ctx *stopper.Context) {
	// We use an interval to ensure that this metric will tick at a
	// reasonable rate in the idle condition.
	const tick = 1 * time.Second

	// Export the min and max resolved timestamps that we see and
	// include lag computations. This happens on each node, regardless
	// of whether it holds a lease.
	ctx.Go(func() error {
		labels := []string{c.factory.kind, c.target.Raw()}
		min := resolvedMinTimestamp.WithLabelValues(labels...)
		max := resolvedMaxTimestamp.WithLabelValues(labels...)
		sourceLag := sourceLagDuration.WithLabelValues(labels...)
		targetLag := targetLagDuration.WithLabelValues(labels...)
		_, err := stopvar.DoWhenChangedOrInterval(ctx,
			hlc.RangeEmpty(), &c.resolvingRange, tick,
			func(ctx *stopper.Context, _, new hlc.Range) error {
				min.Set(float64(new.Min().Nanos()) / 1e9)
				targetLag.Set(float64(time.Now().UnixNano()-new.Min().Nanos()) / 1e9)
				max.Set(float64(new.MaxInclusive().Nanos()) / 1e9)
				sourceLag.Set(float64(time.Now().UnixNano()-new.MaxInclusive().Nanos()) / 1e9)
				return nil
			})
		return err
	})
}

func (c *Conveyor) modeSelector(ctx *stopper.Context) {
	if c.factory.cfg.Immediate {
		c.mode.Set(switcher.ModeImmediate)
		return
	} else if c.factory.cfg.BestEffortOnly {
		c.mode.Set(switcher.ModeBestEffort)
		return
	}
	// The initial update will be async, so wait for it.
	_, initialSet := c.mode.Get()
	ctx.Go(func() error {
		_, err := stopvar.DoWhenChangedOrInterval(ctx,
			hlc.RangeEmptyAt(hlc.New(-1, -1)), // Pick a non-zero time so the callback fires.
			&c.resolvingRange,
			10*time.Second, // Re-evaluate to allow un-jamming big serial transactions.
			func(ctx *stopper.Context, _, bounds hlc.Range) error {
				minTime := time.Unix(0, bounds.Min().Nanos())
				lag := time.Since(minTime)

				// Sometimes you don't know what you want.
				want := switcher.ModeUnknown
				if c.factory.cfg.BestEffortWindow <= 0 {
					// Force a consistent mode.
					want = switcher.ModeConsistent
				} else if lag >= c.factory.cfg.BestEffortWindow {
					// Fallen behind, switch to best-effort.
					want = switcher.ModeBestEffort
				} else if lag <= c.factory.cfg.BestEffortWindow/4 {
					// Caught up close-enough to the current time.
					want = switcher.ModeConsistent
				}

				_, _, _ = c.mode.Update(func(current switcher.Mode) (switcher.Mode, error) {
					// Pick a reasonable default for uninitialized case.
					// Choosing BestEffort here allows us to optimize
					// for the case where a user creates a changefeed
					// that's going to perform a large backfill.
					if current == switcher.ModeUnknown && want == switcher.ModeUnknown {
						want = switcher.ModeBestEffort
					} else if want == switcher.ModeUnknown || current == want {
						// No decision above or no change.
						return current, notify.ErrNoUpdate
					}

					log.Tracef("setting group %s mode to %s", c.target, want)
					return want, nil
				})
				return nil
			})
		return err
	})
	<-initialSet
}

// updateResolved will monitor the timestamp to which tables in the
// group have advanced and update the resolved timestamp table.
func (c *Conveyor) updateResolved(ctx *stopper.Context) {
	ctx.Go(func() error {
		_, err := stopvar.DoWhenChanged(ctx,
			nil,
			c.stat,
			func(ctx *stopper.Context, old, new sequencer.Stat) error {
				oldMin := sequencer.CommonProgress(old)
				newMin := sequencer.CommonProgress(new)

				// Not an interesting change since the minimum didn't advance.
				if oldMin == newMin {
					return nil
				}

				// Mark the range as being complete.
				if err := c.checkpoint.Commit(ctx, newMin); err != nil {
					log.WithError(err).Warnf(
						"could not store updated resolved timestamp for %s; will continue",
						c.target,
					)
				}
				return nil
			})
		return err
	})
}
