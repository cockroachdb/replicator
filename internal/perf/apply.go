// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package perf

import (
	"context"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	applyDurations = promauto.NewSummaryVec(prometheus.SummaryOpts{
		Name:       "apply_duration_seconds",
		Help:       "the length of time it took to successfully apply mutations",
		Objectives: objectives,
	}, tableLabels)
	applyErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "apply_errors_total",
		Help: "the number of times an error was encountered while applying mutations",
	}, tableLabels)
	applyMutations = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "apply_mutations_total",
		Help: "the number of rows upserted",
	}, tableLabels)
)

// Appliers returns a wrapper around the factory that adds
// performance-monitoring around the API.
func Appliers(a types.Appliers) types.Appliers {
	return &appliers{a}
}

type appliers struct {
	delegate types.Appliers
}

func (a *appliers) Get(ctx context.Context, target ident.Table) (types.Applier, error) {
	ret, err := a.delegate.Get(ctx, target)
	if ret != nil {
		ret = &applier{ret, tableValues(target)}
	}
	return ret, err
}

type applier struct {
	types.Applier
	labels []string
}

func (a *applier) Apply(ctx context.Context, batcher types.Batcher, muts []types.Mutation) error {
	start := time.Now()
	err := a.Applier.Apply(ctx, batcher, muts)
	d := time.Since(start)
	if err == nil {
		applyDurations.WithLabelValues(a.labels...).Observe(d.Seconds())
		applyMutations.WithLabelValues(a.labels...).Add(float64(len(muts)))
	} else {
		applyErrors.WithLabelValues(a.labels...).Inc()
	}
	return err
}
