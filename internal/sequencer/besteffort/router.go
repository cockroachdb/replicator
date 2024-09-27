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

package besteffort

import (
	"context"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/pkg/errors"
)

// A router sends mutations to the sequencer associated with a target
// table component.
type router struct {
	config *notify.Var[*routerConfig]
}

type routerConfig struct {
	routes     map[*types.SchemaComponent]types.MultiAcceptor
	schemaData *types.SchemaData
	shutdown   func() error
}

var _ types.MultiAcceptor = (*router)(nil)

// AcceptMultiBatch splits the batch based on destination tables.
func (r *router) AcceptMultiBatch(
	ctx context.Context, batch *types.MultiBatch, opts *types.AcceptOptions,
) error {
	cfg, _ := r.config.Get()
	componentBatches := make(map[*types.SchemaComponent]*types.MultiBatch, len(cfg.routes))

	// Aggregate sub-batches by destination table group.
	for table, mut := range batch.Mutations() {
		comp, ok := cfg.schemaData.TableComponents.Get(table)
		if !ok {
			return errors.Errorf("unknown table %s", table)
		}

		acc, ok := componentBatches[comp]
		if !ok {
			acc = &types.MultiBatch{}
			componentBatches[comp] = acc
		}
		if err := acc.Accumulate(table, mut); err != nil {
			return err
		}
	}

	for comp, acc := range componentBatches {
		destination, ok := cfg.routes[comp]
		if !ok {
			return errors.Errorf("encountered unmapped schema component %s", comp)
		}
		if err := destination.AcceptMultiBatch(ctx, acc, opts); err != nil {
			return err
		}
	}

	return nil
}

// AcceptTableBatch passes through to the table's acceptor.
func (r *router) AcceptTableBatch(
	ctx context.Context, batch *types.TableBatch, opts *types.AcceptOptions,
) error {
	cfg, _ := r.config.Get()
	comp, ok := cfg.schemaData.TableComponents.Get(batch.Table)
	if !ok {
		return errors.Errorf("unknown table %s", batch.Table)
	}
	dest, ok := cfg.routes[comp]
	if !ok {
		return errors.Errorf("unknown component %s", comp)
	}
	return dest.AcceptTableBatch(ctx, batch, opts)
}

// AcceptTemporalBatch delegates to AcceptMultiBatch.
func (r *router) AcceptTemporalBatch(
	ctx context.Context, batch *types.TemporalBatch, opts *types.AcceptOptions,
) error {
	multi := &types.MultiBatch{
		ByTime: map[hlc.Time]*types.TemporalBatch{batch.Time: batch},
		Data:   []*types.TemporalBatch{batch},
	}
	return r.AcceptMultiBatch(ctx, multi, opts)
}
