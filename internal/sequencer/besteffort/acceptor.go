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
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/metrics"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type acceptor struct {
	*BestEffort
	delegate types.TableAcceptor
}

var _ types.TableAcceptor = (*acceptor)(nil)

// AcceptTableBatch implements [types.TableAcceptor]. This
// implementation ignores the options, since we always want to use the
// staging or target database pools.
func (a *acceptor) AcceptTableBatch(
	ctx context.Context, batch *types.TableBatch, _ *types.AcceptOptions,
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	eg, egCtx := errgroup.WithContext(ctx)
	if p := a.cfg.Parallelism; p > 0 {
		eg.SetLimit(p)
	}

	if err := a.applyOrStage(egCtx, eg, batch, a.delegate); err != nil {
		return err
	}
	return eg.Wait()
}

// Unwrap is an informal protocol to return the delegate.
func (a *acceptor) Unwrap() types.TableAcceptor {
	return a.delegate
}

// applyOrStage will write mutations directly into the destination
// table or write them into a staging table.
//
// This method will stage mutations if a staged mutation for the key
// already exists. This ensures that per-key data flows in an ordered
// fashion.
//
// If the attempt to write the remainder of the batch fails and
// [isDeferrableError] returns true, this method will retry each
// mutation individually. If an individual mutation cannot be written,
// again due to a deferrable error, it will be written instead a staging
// table.
//
// The errgroup argument is used to control overall parallelism when
// operating at the individual row level.
func (a *acceptor) applyOrStage(
	ctx context.Context, eg *errgroup.Group, batch *types.TableBatch, acceptor types.TableAcceptor,
) error {
	start := time.Now()
	if len(batch.Data) == 0 {
		return nil
	}
	tblValues := metrics.TableValues(batch.Table)
	appliedCount := acceptAppliedCount.WithLabelValues(tblValues...)
	deferredCount := acceptDeferredCount.WithLabelValues(tblValues...)
	duration := acceptDuration.WithLabelValues(tblValues...)
	errCount := acceptErrors.WithLabelValues(tblValues...)

	stager, err := a.stagers.Get(ctx, batch.Table)
	if err != nil {
		return err
	}

	// If a key already has a pending mutation, we want to stage the
	// next mutation for the key. This call will return the mutations
	// that we still want to attempt to apply.
	attempt, err := stager.StageIfExists(ctx, a.stagingPool, batch.Data)
	if err != nil {
		return errors.WithStack(err)
	}
	deferredCount.Add(float64(len(batch.Data) - len(attempt)))

	// All mutations were already blocked by another mutation.
	if len(attempt) == 0 {
		return nil
	}

	// Try to apply multiple mutations in a single batch. This should
	// generally succeed in no-FK or FK-to-reference kinds of use-cases.
	if len(attempt) > 1 {
		batch = batch.Copy()
		batch.Data = attempt
		if err := acceptor.AcceptTableBatch(ctx, batch, &types.AcceptOptions{}); err == nil {
			// If we can apply all mutations, then we're done.
			appliedCount.Add(float64(len(attempt)))
			duration.Observe(time.Since(start).Seconds())
			return nil
		} else if !isDeferrableError(err) {
			// If the error isn't deferrable, we want to fail out now.
			errCount.Inc()
			return errors.WithStack(err)
		}
	}

	// Apply or stage each mutation individually.
	for idx := range attempt {
		singleBatch := batch.Empty()
		singleBatch.Data = []types.Mutation{batch.Data[idx]}
		eg.Go(func() error {
			if err := acceptor.AcceptTableBatch(ctx, singleBatch, &types.AcceptOptions{}); err == nil {
				// This mutation was applied, so continue onwards.
				appliedCount.Inc()
				return nil
			} else if !isDeferrableError(err) {
				errCount.Inc()
				return errors.WithStack(err)
			}
			if err := stager.Stage(ctx, a.stagingPool, singleBatch.Data); err != nil {
				errCount.Inc()
				return errors.WithStack(err)
			}
			deferredCount.Inc()
			return nil
		})
	}
	return nil
}
