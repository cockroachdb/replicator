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

// Package switcher contains a meta-Sequencer that switches between
// various modes of operation.
package switcher

import (
	"fmt"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/sequencer/besteffort"
	"github.com/cockroachdb/replicator/internal/sequencer/core"
	"github.com/cockroachdb/replicator/internal/sequencer/immediate"
	"github.com/cockroachdb/replicator/internal/sequencer/script"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/diag"
	"github.com/pkg/errors"
)

// Mode describes a strategy using another sequencer.
//
//go:generate go run golang.org/x/tools/cmd/stringer -type=Mode
type Mode int

// The modes of operation that can be dynamically selected from.
const (
	ModeUnknown Mode = iota
	ModeBestEffort
	ModeConsistent
	ModeImmediate

	MaxMode = iota - 1 // Used for testing all modes.
	MinMode = Mode(1)  // Used for testing all modes.
)

// Switcher switches between delegate sequencers. It also adds script
// bindings into the sequencer stack.
type Switcher struct {
	bestEffort  *besteffort.BestEffort
	core        *core.Core
	diags       *diag.Diagnostics
	immediate   *immediate.Immediate
	script      *script.Sequencer
	stagingPool *types.StagingPool
	targetPool  *types.TargetPool

	mode *notify.Var[Mode] // Set by WithMode.
}

var _ sequencer.Sequencer = (*Switcher)(nil)

// Start a sequencer that can switch between various modes of operation.
// Callers must call [Switcher.WithMode] before calling this method.
func (s *Switcher) Start(
	ctx *stopper.Context, opts *sequencer.StartOptions,
) (types.MultiAcceptor, *notify.Var[sequencer.Stat], error) {
	mode := s.mode
	if mode == nil {
		return nil, nil, errors.New("call WithMode() first")
	}
	// Ensure the group is ready to go before returning. Otherwise,
	// the accept methods wouldn't have anywhere to send mutations to.
	initialMode, _ := mode.Get()
	if initialMode == ModeUnknown {
		return nil, nil, errors.New("the mode variable must be set before calling Start")
	}

	g := &groupSequencer{
		Switcher: s,
		group:    opts.Group,
		mode:     mode,
	}

	diagName := fmt.Sprintf("switcher-%s", g.group.Name.Raw())
	if err := s.diags.Register(diagName, g); err != nil {
		return nil, nil, err
	}
	ctx.Defer(func() {
		s.diags.Unregister(diagName)
	})

	ret, err := s.script.Wrap(ctx, g)
	if err != nil {
		return nil, nil, err
	}
	return ret.Start(ctx, opts)
}

// WithMode returns a copy of the Switcher that uses the given variable
// for mode control. This method exists so that Switcher can satisfy the
// [sequencer.Sequencer] interface.
func (s *Switcher) WithMode(mode *notify.Var[Mode]) *Switcher {
	ret := *s
	ret.mode = mode
	return &ret
}
