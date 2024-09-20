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

// Package sequencer contains a number of sub-packages that implement
// various strategies for applying staged mutations.
package sequencer

import (
	"math"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
)

// A Sequencer implements a lifecycle strategy for mutations. The
// various strategies may include immediate, best-effort, or
// fully-transactional behaviors.
//
// The Sequencer type should also allow for composition of behaviors,
// e.g.: userscript dispatch.
type Sequencer interface {
	// Start any goroutines necessary for operating within a
	// [types.TableGroup] and return an acceptor for admitting mutations
	// into that group. The Sequencer will monitor the
	// [StartOptions.Bounds] for updates and periodically emit a Stat
	// which reflects the Sequencer's current state.  It is reasonable
	// to extend the maximum value of the bounds over time, but the
	// minimum value should not be advanced until all tables in the Stat
	// have advanced to at least some common minimum value. When
	// switching between different Sequencers at runtime, callers should
	// discontinue updates to the bounds and ideally wait for the Stat
	// variable to catch up.
	Start(ctx *stopper.Context, opts *StartOptions) (types.MultiAcceptor, *notify.Var[Stat], error)
}

// A Shim allows the behaviors of another Sequencer to be modified.
type Shim interface {
	// Wrap will modify the delegate's behavior. The shim should call
	// [StartOptions.Copy] if the options passed to [Sequencer.Start]
	// are to be modified before passing them to the delegate.
	Wrap(ctx *stopper.Context, delegate Sequencer) (Sequencer, error)
}

// StartOptions is passed to [Sequencer.Start].
type StartOptions struct {
	BatchReader types.BatchReader      // An asynchronous source of transactional data.
	Bounds      *notify.Var[hlc.Range] // Control the range of eligible timestamps.
	Delegate    types.MultiAcceptor    // The acceptor to use when continuing to process mutations.
	Group       *types.TableGroup      // The tables that should be operated on.
	MaxDeferred int                    // Back off after deferring this many mutations.
}

// Copy returns a deep copy of the options.
func (o *StartOptions) Copy() *StartOptions {
	ret := *o
	ret.Group = ret.Group.Copy()
	return &ret
}

// Stat is an interface to allow for more specialized types to aid in
// testing.
type Stat interface {
	Copy() Stat                           // Copy returns a deep copy of the Stat.
	Group() *types.TableGroup             // The TableGroup that was passed to [Sequencer.Start].
	Progress() *ident.TableMap[hlc.Range] // The times to which the members of the group have advanced.
}

// NewStat constructs a basic Stat.
func NewStat(group *types.TableGroup, progress *ident.TableMap[hlc.Range]) Stat {
	return &stat{group, progress}
}

// CommonProgress returns the minimum progress across all tables within
// the [Stat.Group]. If no progress has been made for one or more tables
// in the group, or if the group is empty, [hlc.RangeEmpty] will be
// returned.
func CommonProgress(s Stat) hlc.Range {
	if s == nil || len(s.Group().Tables) == 0 {
		return hlc.RangeEmpty()
	}
	group := s.Group()
	progress := s.Progress()

	commonMax := hlc.New(math.MaxInt64, math.MaxInt)
	for _, table := range group.Tables {
		ts, ok := progress.Get(table)
		if !ok {
			return hlc.RangeEmpty()
		}
		if hlc.Compare(ts.Max(), commonMax) < 0 {
			commonMax = ts.Max()
		}
	}
	return hlc.RangeExcluding(hlc.Zero(), commonMax)
}

type stat struct {
	group    *types.TableGroup
	progress *ident.TableMap[hlc.Range]
}

func (s *stat) Copy() Stat {
	nextProgress := &ident.TableMap[hlc.Range]{}
	s.progress.CopyInto(nextProgress)
	return &stat{
		group:    s.group,
		progress: nextProgress,
	}
}

func (s *stat) Group() *types.TableGroup             { return s.group }
func (s *stat) Progress() *ident.TableMap[hlc.Range] { return s.progress }
