// Copyright 2023 The Cockroach Authors
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

package apply

import (
	"context"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/applycfg"
	"github.com/cockroachdb/cdc-sink/internal/util/diag"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/google/wire"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideAcceptor,
	ProvideFactory,
)

// ProvideAcceptor is called by Wire.
func ProvideAcceptor(
	ctx *stopper.Context,
	cache *types.TargetStatements,
	configs *applycfg.Configs,
	diags *diag.Diagnostics,
	dlqs types.DLQs,
	target *types.TargetPool,
	watchers types.Watchers,
) (*Acceptor, error) {
	f := &factory{
		cache:    cache,
		configs:  configs,
		dlqs:     dlqs,
		product:  target.Product,
		stop:     ctx,
		watchers: watchers,
	}
	f.mu.instances = &ident.TableMap[*apply]{}
	if err := diags.Register("apply", f); err != nil {
		return nil, err
	}

	a := &Acceptor{
		factory:    f,
		targetPool: target,
	}
	return a, nil
}

// ProvideFactory will be removed in an upcoming change.
func ProvideFactory(acc *Acceptor) types.Appliers {
	return &appliersAdapter{acc.factory}
}

// This will be removed in a subsequent commit.
type appliersAdapter struct {
	f *factory
}

var _ types.Appliers = (*appliersAdapter)(nil)

// Get implements [types.Appliers].
func (a *appliersAdapter) Get(_ context.Context, target ident.Table) (types.Applier, error) {
	return &applierAdapter{a.f, target}, nil
}

// This will be removed in a subsequent commit.
type applierAdapter struct {
	f      *factory
	target ident.Table
}

var _ types.Applier = (*applierAdapter)(nil)

func (a *applierAdapter) Apply(
	ctx context.Context, querier types.TargetQuerier, mutations []types.Mutation,
) error {
	app, err := a.f.Get(ctx, a.target)
	if err != nil {
		return err
	}
	return app.Apply(ctx, querier, mutations)
}
