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

// Package seqtest provides a test fixture for instantiating sequencers
// and other general-purpose test helpers.
package seqtest

import (
	"context"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/sequencer/besteffort"
	"github.com/cockroachdb/replicator/internal/sequencer/chaos"
	"github.com/cockroachdb/replicator/internal/sequencer/core"
	"github.com/cockroachdb/replicator/internal/sequencer/immediate"
	"github.com/cockroachdb/replicator/internal/sequencer/retire"
	"github.com/cockroachdb/replicator/internal/sequencer/script"
	"github.com/cockroachdb/replicator/internal/sequencer/switcher"
	"github.com/cockroachdb/replicator/internal/sinktest/all"
	"github.com/cockroachdb/replicator/internal/staging/leases"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/pkg/errors"
)

// Fixture provides ready-to-use instances of sequencer types.
type Fixture struct {
	*all.Fixture

	BestEffort *besteffort.BestEffort
	Chaos      *chaos.Chaos
	Core       *core.Core
	Immediate  *immediate.Immediate
	Retire     *retire.Retire
	Script     *script.Sequencer
	Switcher   *switcher.Switcher
}

// SequencerFor returns a Sequencer instance that corresponds to the
// given mode enum.
func (f *Fixture) SequencerFor(
	ctx *stopper.Context, mode switcher.Mode,
) (sequencer.Sequencer, error) {
	switch mode {
	case switcher.ModeBestEffort:
		return f.BestEffort.Wrap(ctx, f.Core)
	case switcher.ModeConsistent:
		return f.Core, nil
	case switcher.ModeImmediate:
		return f.Immediate, nil
	default:
		return nil, errors.Errorf("unimplemented, %s", mode)
	}
}

func provideLeases(
	ctx context.Context, pool *types.StagingPool, stagingDB ident.StagingSchema,
) (types.Leases, error) {
	return leases.New(ctx, leases.Config{
		Pool:   pool,
		Target: ident.NewTable(stagingDB.Schema(), ident.New("leases")),
	})
}
