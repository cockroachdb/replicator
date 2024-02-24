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

	"github.com/cockroachdb/cdc-sink/internal/sequencer"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/besteffort"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/chaos"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/immediate"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/script"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/serial"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/shingle"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/diag"
	"github.com/cockroachdb/cdc-sink/internal/util/notify"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
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
	ModeImmediate
	ModeSerial
	ModeShingle

	MinMode = Mode(1)     // Used for testing all modes.
	MaxMode = ModeShingle // Used for testing all modes.
)

// Switcher switches between delegate sequencers. It also adds script
// bindings into the sequencer stack.
type Switcher struct {
	bestEffort  *besteffort.BestEffort
	chaos       *chaos.Chaos
	diags       *diag.Diagnostics
	immediate   *immediate.Immediate
	serial      *serial.Serial
	script      *script.Sequencer
	shingle     *shingle.Shingle
	stagingPool *types.StagingPool
	targetPool  *types.TargetPool
}

// Start a sequencer that can switch between various modes of operation.
func (s *Switcher) Start(
	ctx *stopper.Context, opts *sequencer.StartOptions, mode *notify.Var[Mode],
) (types.MultiAcceptor, *notify.Var[sequencer.Stat], error) {
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
	ret, err = s.chaos.Wrap(ctx, ret)
	if err != nil {
		return nil, nil, err
	}
	return ret.Start(ctx, opts)
}
