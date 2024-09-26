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

package switcher

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/field-eng-powertools/stopvar"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/diag"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// groupSequencer manages a single call to [Switcher.Start].
type groupSequencer struct {
	*Switcher
	group  *types.TableGroup
	mode   *notify.Var[Mode]
	status notify.Var[sequencer.Stat]

	// The members of this struct are replaced whenever we switch modes.
	// A read-lock is held whenever a batch is being processed by the
	// active acceptor.
	mu struct {
		sync.RWMutex
		acceptor types.MultiAcceptor // Created by the downstream sequencer.
		stopper  *stopper.Context    // Stopped and replaced during mode switch.
	}
}

var (
	_ diag.Diagnostic     = (*groupSequencer)(nil)
	_ sequencer.Sequencer = (*groupSequencer)(nil)
)

// Diagnostic implements [diag.Diagnostic].
func (g *groupSequencer) Diagnostic(context.Context) any {
	ret := struct {
		Group *types.TableGroup
		Mode  Mode
		Stat  sequencer.Stat
	}{
		Group: g.group,
	}
	ret.Mode, _ = g.mode.Get()
	ret.Stat, _ = g.status.Get()

	return &ret
}

// Start implements [sequencer.Sequencer].
func (g *groupSequencer) Start(
	ctx *stopper.Context, opts *sequencer.StartOptions,
) (*notify.Var[sequencer.Stat], error) {
	// There's a sanity check in the main Sequencer type to ensure that
	// a mode has already been set. This ensures that we can return a
	// ready-to-run instance of the groupSequencer.
	initialMode, _ := g.mode.Get()
	log.Infof("%s: initial mode is %s", g.group, initialMode)
	g.mu.Lock()
	err := g.switchModeLocked(ctx, opts, initialMode)
	g.mu.Unlock()
	if err != nil {
		return nil, err
	}

	ctx.Go(func(ctx *stopper.Context) error {
		_, err := stopvar.DoWhenChanged(ctx, initialMode, g.mode,
			func(ctx *stopper.Context, old, new Mode) error {
				g.mu.Lock()
				defer g.mu.Unlock()
				if err := g.switchModeLocked(ctx, opts, new); err == nil {
					log.Infof("%s: mode changed %s -> %s", g.group, old, new)
				} else {
					log.WithError(err).Warnf("%s: unable to change mode %s -> %s; continuing", g.group, old, new)
				}
				return nil
			})

		g.mu.Lock()
		g.mu.acceptor = nil
		g.mu.Unlock()

		return err
	})
	return &g.status, nil
}

func (g *groupSequencer) switchModeLocked(
	ctx *stopper.Context, opts *sequencer.StartOptions, next Mode,
) error {
	if g.mu.stopper != nil {
		log.Tracef("%s: waiting for previous epoch to complete", g.group)
		g.mu.stopper.Stop(time.Minute)
		if err := g.mu.stopper.Wait(); err != nil {
			log.WithError(err).Warnf("error during mode transition to %s", next)
		} else {
			log.Tracef("%s: previous epoch has completed", g.group)
		}
	}

	// Create a nested stopper context to execute tasks in.
	g.mu.stopper = stopper.WithContext(ctx)

	// Route incoming batches of data.
	var err error
	var nextSeq sequencer.Sequencer
	switch next {
	case ModeBestEffort:
		nextSeq, err = g.staging.Wrap(ctx, g.core)
		if err != nil {
			return err
		}
		nextSeq, err = g.bestEffort.Wrap(ctx, nextSeq)
	case ModeImmediate:
		nextSeq = g.immediate
		// Immediate doesn't progess staged data.
		opts = opts.Copy()
		opts.BatchReader = nil
	case ModeConsistent:
		nextSeq, err = g.staging.Wrap(ctx, g.core)
	default:
		return errors.Errorf("unimplemented: %v", next)
	}
	if err != nil {
		return err
	}
	nextStatus, err := nextSeq.Start(g.mu.stopper, opts)
	if err != nil {
		return err
	}
	// XXX reader that switches.
	// g.mu.acceptor = nextAcc

	// Copy the status out to the persistent notification variable.
	g.mu.stopper.Go(func(ctx *stopper.Context) error {
		_, err := stopvar.DoWhenChanged(ctx, nil, nextStatus,
			func(ctx *stopper.Context, _, new sequencer.Stat) error {
				g.status.Set(new)
				return nil
			})
		return err
	})
	return nil
}
