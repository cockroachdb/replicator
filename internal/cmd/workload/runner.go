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

// Package workload provides a basic parent/child table workload.
package workload

import (
	"context"
	"time"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/source/cdc"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/lockset"
	"github.com/cockroachdb/replicator/internal/util/workload"
	log "github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

type runner struct {
	cfg          *clientConfig
	gen          *workload.GeneratorBase
	lastResolved hlc.Time
	lastTime     hlc.Time // Ensures distinct values from hlcNow.
	limiter      *rate.Limiter
	sender       *sender
}

func newRunner(
	ctx context.Context, cfg *clientConfig, gen *workload.GeneratorBase,
) (*runner, error) {
	sender, err := newSender(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &runner{
		cfg:      cfg,
		gen:      gen,
		lastTime: hlc.Time{},
		limiter:  rate.NewLimiter(rate.Limit(cfg.rate), 1),
		sender:   sender,
	}, nil
}

func (r *runner) Run(ctx *stopper.Context) error {
	defer log.Trace("runner has stopped")

	// We'll accumulate task outcomes until the ticker fires.
	var sendResolvedAfter []lockset.Outcome
	resolvedTicker := time.NewTicker(r.cfg.resolvedInterval)
	defer resolvedTicker.Stop()

	// This channel limits the total number outstanding HTTP requests.
	pending := make(chan lockset.Outcome, r.cfg.concurrentRequests)
	defer close(pending)
	ctx.Go(func(ctx *stopper.Context) error {
		for outcome := range pending {
			if err := lockset.Wait(ctx, []lockset.Outcome{outcome}); err != nil {
				log.WithError(err).Error("fatal error")
				ctx.Stop(0)
				return err
			}
		}
		return nil
	})

	// Write an occasional log message.
	const reportInterval = 5 * time.Second
	total := 0
	reportTicker := time.NewTicker(reportInterval)
	defer reportTicker.Stop()

	for {
		count, outcome := r.sendBatch(ctx)
		total += count
		pending <- outcome
		sendResolvedAfter = append(sendResolvedAfter, outcome)

		// Maybe report progress.
		select {
		case <-reportTicker.C:
			log.Infof("sent %d mutations (%.2f mps)", total, float64(total)/reportInterval.Seconds())
			total = 0
		default:
		}

		select {
		case <-ctx.Stopping():
			// Send a final checkpoint to allow the test to complete.
			resolved := r.hlcNow()
			r.lastResolved = resolved
			final := r.sendResolved(ctx, resolved, sendResolvedAfter)
			return lockset.Wait(ctx, []lockset.Outcome{final})

		case <-resolvedTicker.C:
			// Enough time has elapsed that we want to send a checkpoint.
			resolved := r.hlcNow()
			r.lastResolved = resolved
			waitFor := sendResolvedAfter
			sendResolvedAfter = nil
			pending <- r.sendResolved(ctx, resolved, waitFor)

		default:
			// Keep sending.
		}
	}
}

func (r *runner) sendBatch(ctx *stopper.Context) (int, lockset.Outcome) {
	// Wait until at least one event may be sent.
	if err := r.limiter.Wait(ctx); err != nil {
		return 0, notify.VarOf(lockset.StatusFor(err))
	}
	batch := &types.MultiBatch{}
	for range r.cfg.batchSize {
		r.gen.GenerateInto(batch, r.hlcNow())

		// Send a smaller payload if we've hit the rate limit.
		if !r.limiter.Allow() {
			break
		}
	}
	payload, err := cdc.NewWebhookPayload(batch)
	if err != nil {
		return 0, notify.VarOf(lockset.StatusFor(err))
	}
	return batch.Count(), r.sender.Send(ctx, payload)
}

func (r *runner) sendResolved(
	ctx *stopper.Context, ts hlc.Time, waitFor []lockset.Outcome,
) lockset.Outcome {
	if err := lockset.Wait(ctx, waitFor); err != nil {
		return notify.VarOf(lockset.StatusFor(err))
	}
	payload, err := cdc.NewWebhookResolved(ts)
	if err != nil {
		return notify.VarOf(lockset.StatusFor(err))
	}
	return r.sender.Send(ctx, payload)
}

// hlcNow returns a new HLC time that is always greater than the one
// returned previously. It will increment the logical counter if the
// wall time has not advanced.
func (r *runner) hlcNow() hlc.Time {
	nextNanos := time.Now().UnixNano()
	if nextNanos > r.lastTime.Nanos() {
		r.lastTime = hlc.New(nextNanos, 0)
		return r.lastTime
	}
	r.lastTime = r.lastTime.Next()
	return r.lastTime
}
