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

// Package shingle contains a [sequencer.Shim] that allows transactions
// at multiple timestamps to be applied concurrently.
package shingle

import (
	"github.com/cockroachdb/cdc-sink/internal/sequencer"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/notify"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
)

// Shingle injects a downstream [types.MultiAcceptor] which allows each
// target transaction to be applied concurrently, with ordering enforced
// on a per-key basis.
type Shingle struct {
	cfg     *sequencer.Config
	stagers types.Stagers
	staging *types.StagingPool
	target  *types.TargetPool
}

var _ sequencer.Shim = (*Shingle)(nil)

// Wrap implements [sequencer.Shim]. If no parallelism is allowed by the
// configuration, the delegate is returned.
func (s *Shingle) Wrap(
	_ *stopper.Context, delegate sequencer.Sequencer,
) (sequencer.Sequencer, error) {
	if s.cfg.Parallelism <= 1 {
		return delegate, nil
	}
	return &shingle{s, delegate}, nil
}

type shingle struct {
	*Shingle
	delegate sequencer.Sequencer
}

var _ sequencer.Sequencer = (*shingle)(nil)

// Start implements [sequencer.Sequencer].
func (s *shingle) Start(
	ctx *stopper.Context, opts *sequencer.StartOptions,
) (types.MultiAcceptor, *notify.Var[sequencer.Stat], error) {
	opts = opts.Copy()
	opts.Delegate = &acceptor{Shingle: s.Shingle, delegate: opts.Delegate}
	return s.delegate.Start(ctx, opts)
}

// Unwrap is an informal protocol to return the delegate.
func (s *shingle) Unwrap() sequencer.Sequencer {
	return s.delegate
}
