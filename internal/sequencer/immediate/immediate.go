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

// Package immediate contains a trivial [sequencer.Sequencer]
// implementation which writes data directly to the configured acceptor.
package immediate

import (
	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/field-eng-powertools/stopvar"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/sequencer/decorators"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
)

// Immediate is a [sequencer.Shim] that guards the output acceptor with
// exactly-only behavior if a non-idempotent source is configured.
type Immediate struct {
	cfg         *sequencer.Config
	marker      *decorators.Marker
	once        *decorators.Once
	retryTarget *decorators.RetryTarget
	stagers     types.Stagers
	targetPool  *types.TargetPool
}

var _ sequencer.Shim = (*Immediate)(nil)

// Wrap implements [sequencer.Shim].
func (i *Immediate) Wrap(_ *stopper.Context, delegate sequencer.Sequencer) (sequencer.Sequencer, error) {
	return &immediate{i, delegate}, nil
}

type immediate struct {
	*Immediate
	delegate sequencer.Sequencer
}

var _ sequencer.Sequencer = (*immediate)(nil)

func (i *immediate) Start(
	ctx *stopper.Context, opts *sequencer.StartOptions,
) (*notify.Var[sequencer.Stat], error) {
	opts = opts.Copy()
	acc := opts.Delegate
	acc = i.retryTarget.MultiAcceptor(acc)
	if !i.cfg.IdempotentSource {
		acc = i.marker.MultiAcceptor(acc)
		acc = i.once.MultiAcceptor(acc)
	}
	opts.Delegate = acc
	_, err := i.delegate.Start(ctx, opts)
	if err != nil {
		return nil, err
	}

	ret := notify.VarOf(sequencer.NewStat(opts.Group, &ident.TableMap[hlc.Range]{}))

	// Set each table's progress to the end of the bounds. This
	// will allow the checkpointer to clean up resolved timestamps.
	ctx.Go(func(ctx *stopper.Context) error {
		_, err := stopvar.DoWhenChanged(ctx, hlc.RangeEmpty(), opts.Bounds,
			func(ctx *stopper.Context, old, new hlc.Range) error {
				// Do nothing if the new end point didn't advance.
				if hlc.Compare(new.Max(), old.Max()) <= 0 {
					return nil
				}

				// Show each table in the group as having advanced to
				// the end of the resolving range.
				nextProgress := &ident.TableMap[hlc.Range]{}
				for _, table := range opts.Group.Tables {
					nextProgress.Put(table, new)
				}
				ret.Set(sequencer.NewStat(opts.Group, nextProgress))
				return nil
			})
		return err
	})
	return ret, nil
}
