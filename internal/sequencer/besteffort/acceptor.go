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

	"github.com/cockroachdb/cdc-sink/internal/sequencer/sequtil"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/lockset"
	"github.com/cockroachdb/cdc-sink/internal/util/metrics"
	"github.com/cockroachdb/cdc-sink/internal/util/notify"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type acceptor struct {
	*BestEffort
	delegate types.TableAcceptor
}

var _ types.TableAcceptor = (*acceptor)(nil)

// AcceptTableBatch implements [types.TableAcceptor]. It will write
// mutations directly into the destination table or write them into a
// staging table. This implementation ignores the options, since we
// always want to use the staging or target database pools.
//
// This method will stage mutations if a staged mutation for the key
// already exists. This ensures that per-key data flows in an ordered
// fashion.
//
// If the attempt to write the remainder of the batch fails, this method
// will retry each mutation individually. If an individual mutation
// cannot be written to the target, it will be written instead a staging
// table. This helps to prevent backpressure on the source if the target
// is malfunctioning.
func (a *acceptor) AcceptTableBatch(
	ctx context.Context, batch *types.TableBatch, _ *types.AcceptOptions,
) error {
	if len(batch.Data) == 0 {
		return nil
	}
	start := time.Now()
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
	acceptor := a.delegate
	if len(attempt) > 1 {
		batch = batch.Copy()
		batch.Data = attempt
		if err := acceptor.AcceptTableBatch(ctx, batch, &types.AcceptOptions{}); err == nil {
			// If we can apply all mutations, then we're done.
			appliedCount.Add(float64(len(attempt)))
			duration.Observe(time.Since(start).Seconds())
			return nil
		}
	}

	// We've encountered some error that may affect one or all of the
	// mutations in the batch. We'll retry each mutation individually in
	// the hope of making partial progress on the target. Any mutations
	// that can't be written to the target can at least be written to
	// the staging table and processed later. This ultimately allows
	// BestEffort to insulate the source (changefeed) from target
	// database malfunction, schema drift, etc. as long as the staging
	// table can be written to. Overall parallelism is limited by
	// the lockset's Runner.
	outcomes := make([]*notify.Var[*lockset.Status], len(attempt))
	for idx := range attempt {
		mut := batch.Data[idx]
		singleBatch := batch.Empty()
		singleBatch.Data = []types.Mutation{mut}

		outcomes[idx] = a.scheduler.Singleton(batch.Table, mut, func() error {
			err := acceptor.AcceptTableBatch(ctx, singleBatch, &types.AcceptOptions{
				TargetQuerier: a.targetPool,
			})
			if err == nil {
				// This mutation was applied, so continue onwards.
				appliedCount.Inc()
				return nil
			}
			deferredCount.Inc()
			// We'll suppress errors like FK constraint violations.
			if !sequtil.IsDeferrableError(err) {
				log.WithError(err).Warnf(
					"staging mutation instead of target table %s key %s",
					singleBatch.Table, string(singleBatch.Data[0].Key))
			}
			if err := stager.Stage(ctx, a.stagingPool, singleBatch.Data); err != nil {
				errCount.Inc()
				return errors.WithStack(err)
			}
			return nil
		})
	}

	return lockset.Wait(ctx, outcomes)
}

// Unwrap is an informal protocol to return the delegate.
func (a *acceptor) Unwrap() types.TableAcceptor {
	return a.delegate
}
