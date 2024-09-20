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

// Package staging contains a [sequencer.Shim] that writes incoming
// mutations to staging.
package staging

import (
	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/sequencer/decorators"
	"github.com/cockroachdb/replicator/internal/types"
)

// Staging is a [sequencer.Shim] that returns an acceptor that writes
// incoming data to staging tables.
type Staging struct {
	cfg         *sequencer.Config
	markers     *decorators.Marker
	stagers     types.Stagers
	stagingPool *types.StagingPool
}

var _ sequencer.Shim = (*Staging)(nil)

// Wrap implements [sequencer.Shim].
func (s *Staging) Wrap(
	_ *stopper.Context, delegate sequencer.Sequencer,
) (sequencer.Sequencer, error) {
	return &staging{s, delegate}, nil
}

type staging struct {
	*Staging
	delegate sequencer.Sequencer
}

var _ sequencer.Sequencer = (*staging)(nil)

// Start injects a staging-table query as an asynchronous data source to
// be passed into the next (core) sequencer.
func (s *staging) Start(
	ctx *stopper.Context, opts *sequencer.StartOptions,
) (types.MultiAcceptor, *notify.Var[sequencer.Stat], error) {
	reader, err := s.stagers.Query(ctx, &types.StagingQuery{
		Bounds:       opts.Bounds,
		FragmentSize: s.cfg.ScanSize,
		Group:        opts.Group,
	})
	if err != nil {
		return nil, nil, err
	}

	opts = opts.Copy()
	opts.BatchReader = reader
	opts.Delegate = s.markers.MultiAcceptor(opts.Delegate)

	_, stats, err := s.delegate.Start(ctx, opts)
	return &acceptor{s.Staging}, stats, err
}
