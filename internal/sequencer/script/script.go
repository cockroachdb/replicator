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

// Package script provides an API shim that integrates the userscript
// into the [sequencer.Sequencer] API.
package script

import (
	"context"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/script"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/ident"
)

// Sequencer injects the userscript shim into a [sequencer.Sequencer]
// stack.
type Sequencer struct {
	loader     *script.Loader
	targetPool *types.TargetPool
	watchers   types.Watchers
}

var _ sequencer.Shim = (*Sequencer)(nil)

// Wrap implements [sequencer.Shim].
func (s *Sequencer) Wrap(
	_ *stopper.Context, delegate sequencer.Sequencer,
) (sequencer.Sequencer, error) {
	return &wrapper{s, delegate}, nil
}

type wrapper struct {
	*Sequencer
	delegate sequencer.Sequencer
}

var _ sequencer.Sequencer = (*wrapper)(nil)

// Start injects a userscript shim into the Sequencer stack.
func (w *wrapper) Start(
	ctx *stopper.Context, opts *sequencer.StartOptions,
) (types.MultiAcceptor, *notify.Var[sequencer.Stat], error) {
	// Loader is nil if no userscript has been configured.
	if w.loader == nil {
		return w.delegate.Start(ctx, opts)
	}

	schema, err := opts.Group.Schema()
	if err != nil {
		return nil, nil, err
	}

	scr, err := w.loader.Bind(ctx, schema, opts.Delegate, w.watchers)
	if err != nil {
		return nil, nil, err
	}

	// Only inject if the source or any tables have a configuration.
	sourceBindings, inject := scr.Sources.Get(opts.Group.Name)
	if !inject {
		for _, tbl := range opts.Group.Tables {
			_, inject = scr.Targets.Get(tbl)
			if inject {
				break
			}
		}
	}
	if !inject {
		return w.delegate.Start(ctx, opts)
	}

	watcher, err := w.watchers.Get(opts.Group.Enclosing)
	if err != nil {
		return nil, nil, err
	}

	// If the userscript has defined any apply functions, we will
	// need to ensure that a database transaction will be available
	// to support the api.getTX() function. This is mainly relevant
	// to immediate mode, in which the sequencer caller won't
	// necessarily have created a transaction.
	ensureTX := false
	// No interesting error returned from Range.
	_ = scr.Targets.Range(func(_ ident.Table, target *script.Target) error {
		if target.UserAcceptor != nil {
			ensureTX = true
			return context.Canceled // Arbitrary error to stop early.
		}
		return nil
	})

	// Install the target-phase acceptor into the options chain. This
	// will be invoked for mutations which have passed through the
	// sequencer stack.
	opts = opts.Copy()
	opts.Delegate = types.OrderedAcceptorFrom(&targetAcceptor{
		delegate:   opts.Delegate,
		ensureTX:   ensureTX,
		group:      opts.Group,
		targetPool: w.targetPool,
		userScript: scr,
	}, w.watchers)

	// Initialize downstream sequencer.
	acc, stat, err := w.delegate.Start(ctx, opts)
	if err != nil {
		return nil, nil, err
	}

	// Install the source-phase acceptor. This provides the user with
	// the opportunity to rewrite mutations before they are presented to
	// the upstream sequencer.
	acc = types.UnorderedAcceptorFrom(&sourceAcceptor{
		delegate:       acc,
		group:          opts.Group,
		sourceBindings: sourceBindings,
		watcher:        watcher,
	})
	return acc, stat, nil
}

// Unwrap is an informal protocol to return the delegate.
func (w *wrapper) Unwrap() sequencer.Sequencer {
	return w.delegate
}
