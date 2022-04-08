// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logical

import (
	"context"
	"math/rand"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	"github.com/pkg/errors"
)

// ErrChaos is the error that will be injected by the WithChaos wrappers
// in this package.
var ErrChaos = errors.New("chaos")

// WithChaos returns a wrapper around a Dialect that will inject errors
// at various points throughout the execution.
func WithChaos(delegate Dialect, prob float32) Dialect {
	return &chaosDialect{
		delegate: delegate,
		prob:     prob,
		r:        rand.New(rand.NewSource(0)),
	}
}

type chaosDialect struct {
	delegate Dialect
	prob     float32
	r        *rand.Rand
}

var _ Dialect = (*chaosDialect)(nil)

func (d *chaosDialect) ReadInto(ctx context.Context, ch chan<- Message, state State) error {
	if d.r.Float32() < d.prob {
		return ErrChaos
	}
	return d.delegate.ReadInto(ctx, ch, state)
}

func (d *chaosDialect) Process(ctx context.Context, ch <-chan Message, events Events) error {
	if d.r.Float32() < d.prob {
		return ErrChaos
	}
	return d.delegate.Process(ctx, ch, &chaosEvents{events, d.prob, d.r})
}

type chaosEvents struct {
	delegate Events
	prob     float32
	r        *rand.Rand
}

var _ Events = (*chaosEvents)(nil)

func (e *chaosEvents) GetConsistentPoint() stamp.Stamp {
	return e.delegate.GetConsistentPoint()
}

func (e *chaosEvents) GetTargetDB() ident.Ident {
	return e.delegate.GetTargetDB()
}

func (e *chaosEvents) OnBegin(ctx context.Context, point stamp.Stamp) error {
	if e.r.Float32() < e.prob {
		return ErrChaos
	}
	return e.delegate.OnBegin(ctx, point)
}

func (e *chaosEvents) OnCommit(ctx context.Context) error {
	if e.r.Float32() < e.prob {
		return ErrChaos
	}
	return e.delegate.OnCommit(ctx)
}

func (e *chaosEvents) OnData(ctx context.Context, target ident.Table, muts []types.Mutation) error {
	if e.r.Float32() < e.prob {
		return ErrChaos
	}
	return e.delegate.OnData(ctx, target, muts)
}

func (e *chaosEvents) OnRollback(ctx context.Context, msg Message) error {
	if e.r.Float32() < e.prob {
		return ErrChaos
	}
	return e.delegate.OnRollback(ctx, msg)
}
