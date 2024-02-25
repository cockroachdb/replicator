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

package cdc

import (
	"sync"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/sequencer"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/retire"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/switcher"
	"github.com/cockroachdb/cdc-sink/internal/staging/checkpoint"
	"github.com/cockroachdb/cdc-sink/internal/target/apply"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/metrics"
	"github.com/cockroachdb/cdc-sink/internal/util/notify"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/cockroachdb/cdc-sink/internal/util/stopvar"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"
)

type targetInfo struct {
	acceptor       types.MultiAcceptor         // Possibly-async writes to the target.
	checkpoint     *checkpoint.Group           // Persistence of checkpoint (fka. resolved) timestamps
	mode           notify.Var[switcher.Mode]   // Switchable strategies.
	resolvingRange notify.Var[hlc.Range]       // Range of resolved timestamps to be processed.
	stat           *notify.Var[sequencer.Stat] // Processing status.
	target         ident.Schema                // Identify for logging.
	watcher        types.Watcher               // Schema info.
}

// Targets manages the plumbing necessary to deliver changefeed messages
// to a target schema. It is also responsible for mode-switching.
type Targets struct {
	cfg           *Config
	checkpoints   *checkpoint.Checkpoints
	retire        *retire.Retire
	staging       *types.StagingPool
	stopper       *stopper.Context
	switcher      *switcher.Switcher
	tableAcceptor *apply.Acceptor
	watchers      types.Watchers

	mu struct {
		sync.RWMutex
		targets ident.SchemaMap[*targetInfo]
	}
}

func (t *Targets) getTarget(schema ident.Schema) (*targetInfo, error) {
	t.mu.RLock()
	found, ok := t.mu.targets.Get(schema)
	t.mu.RUnlock()
	if ok {
		return found, nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Double-check.
	if found, ok := t.mu.targets.Get(schema); ok {
		return found, nil
	}

	w, err := t.watchers.Get(schema)
	if err != nil {
		return nil, err
	}
	var tables []ident.Table
	_ = w.Get().Columns.Range(func(tbl ident.Table, _ []types.ColData) error {
		tables = append(tables, tbl)
		return nil
	})

	tableGroup := &types.TableGroup{
		Name:   ident.New(schema.Raw()),
		Tables: tables,
	}

	ret := &targetInfo{
		target:  schema,
		watcher: w,
	}

	ret.checkpoint, err = t.checkpoints.Start(t.stopper, tableGroup, &ret.resolvingRange)
	if err != nil {
		return nil, err
	}

	// Set the mode before starting the switcher.
	t.modeSelector(ret)

	ret.acceptor, ret.stat, err = t.switcher.WithMode(&ret.mode).Start(
		t.stopper,
		&sequencer.StartOptions{
			Bounds:   &ret.resolvingRange,
			Delegate: types.OrderedAcceptorFrom(t.tableAcceptor, t.watchers),
			Group:    tableGroup,
		})
	if err != nil {
		return nil, err
	}

	// Advance the stored resolved timestamps.
	t.updateResolved(ret)

	// Allow old staged mutations to be retired.
	t.retire.Start(t.stopper, tableGroup, &ret.resolvingRange)

	// Report timestamps and lag.
	t.metrics(ret)

	t.mu.targets.Put(schema, ret)
	return ret, nil
}

var (
	lagDuration = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cdc_target_lag_seconds",
		Help: "the age of the data applied to the table",
	}, metrics.TableLabels)
	resolvedMinTimestamp = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cdc_target_applied_timestamp_seconds",
		Help: "the wall time of the most recent applied resolved timestamp",
	}, []string{"target"})
	resolvedMaxTimestamp = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cdc_target_pending_timestamp_seconds",
		Help: "the wall time of the most recently received resolved timestamp",
	}, []string{"target"})
)

func (t *Targets) metrics(info *targetInfo) {
	// We use an interval to ensure that this metric will tick at a
	// reasonable rate in the idle condition.
	const tick = 5 * time.Second

	// Refresh lag values.
	t.stopper.Go(func() error {
		_, err := stopvar.DoWhenChangedOrInterval(t.stopper,
			nil, info.stat, tick,
			func(ctx *stopper.Context, _, new sequencer.Stat) error {
				now := time.Now()
				return new.Progress().Range(func(tbl ident.Table, advanced hlc.Time) error {
					lag := now.Sub(time.Unix(0, advanced.Nanos())).Seconds()
					lagDuration.WithLabelValues(metrics.TableValues(tbl)...).Set(lag)
					return nil
				})
			})
		return err
	})

	// Export the min and max resolved timestamps that we see.
	t.stopper.Go(func() error {
		min := resolvedMinTimestamp.WithLabelValues(info.target.Raw())
		max := resolvedMaxTimestamp.WithLabelValues(info.target.Raw())
		_, err := stopvar.DoWhenChangedOrInterval(t.stopper,
			hlc.RangeEmpty(), &info.resolvingRange, tick,
			func(ctx *stopper.Context, _, new hlc.Range) error {
				min.Set(float64(new.Min().Nanos()) / 1e9)
				max.Set(float64(new.Max().Nanos()) / 1e9)
				return nil
			})
		return err
	})
}

func (t *Targets) modeSelector(info *targetInfo) {
	if t.cfg.Immediate {
		info.mode.Set(switcher.ModeImmediate)
		return
	} else if t.cfg.BestEffortOnly {
		info.mode.Set(switcher.ModeBestEffort)
		return
	}
	// The initial update will be async, so wait for it.
	_, initialSet := info.mode.Get()
	t.stopper.Go(func() error {
		_, err := stopvar.DoWhenChangedOrInterval(t.stopper,
			hlc.RangeEmptyAt(hlc.New(-1, -1)), // Pick a non-zero time so the callback fires.
			&info.resolvingRange,
			10*time.Second, // Re-evaluate to allow un-jamming big serial transactions.
			func(ctx *stopper.Context, _, bounds hlc.Range) error {
				minTime := time.Unix(0, bounds.Min().Nanos())
				lag := time.Since(minTime)

				// Sometimes you don't know what you want.
				want := switcher.ModeUnknown
				if t.cfg.BestEffortWindow <= 0 {
					// Force a consistent mode.
					want = switcher.ModeShingle
				} else if lag >= t.cfg.BestEffortWindow {
					// Fallen behind, switch to best-effort.
					want = switcher.ModeBestEffort
				} else if lag <= t.cfg.BestEffortWindow/4 {
					// Caught up close-enough to the current time.
					want = switcher.ModeShingle
				}

				_, _, _ = info.mode.Update(func(current switcher.Mode) (switcher.Mode, error) {
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

					log.Tracef("setting group %s mode to %s", info.target, want)
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
func (t *Targets) updateResolved(info *targetInfo) {
	t.stopper.Go(func() error {
		_, err := stopvar.DoWhenChanged(t.stopper,
			nil,
			info.stat,
			func(ctx *stopper.Context, old, new sequencer.Stat) error {
				oldMin := sequencer.CommonMin(old)
				newMin := sequencer.CommonMin(new)

				// Not an interesting change since the minimum didn't advance.
				if oldMin == newMin {
					return nil
				}

				_, _, _ = info.resolvingRange.Update(func(window hlc.Range) (new hlc.Range, _ error) {
					// If the new minimum doesn't push the window forward, do nothing.
					if hlc.Compare(newMin, window.Min()) <= 0 {
						return window, notify.ErrNoUpdate
					}

					// Mark the range as being complete.
					complete := hlc.RangeIncluding(window.Min(), newMin)
					if err := info.checkpoint.Commit(ctx, complete); err != nil {
						log.WithError(err).Warnf(
							"could not store updated resolved timestamp for %s; will continue",
							info.target,
						)
						return window, notify.ErrNoUpdate
					}
					log.Tracef("wrote applied_at time for %s: %s", info.target, complete)

					return window.WithMin(newMin), nil
				})
				return nil
			})
		return err
	})
}
