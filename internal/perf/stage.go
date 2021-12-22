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
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	stageDrainCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "stage_drain_mutations_total",
		Help: "the number of mutations drained for this table",
	}, tableLabels)
	stageDrainDurations = promauto.NewSummaryVec(prometheus.SummaryOpts{
		Name:       "stage_drain_duration_seconds",
		Help:       "the length of time it took to successfully drain mutations",
		Objectives: objectives,
	}, tableLabels)
	stageDrainErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "stage_drain_errors_total",
		Help: "the number of times an error was encountered while draining mutations",
	}, tableLabels)

	stageStoreCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "stage_store_mutations_total",
		Help: "the number of mutations stored for this table",
	}, tableLabels)
	stageStoreDurations = promauto.NewSummaryVec(prometheus.SummaryOpts{
		Name:       "stage_store_duration_seconds",
		Help:       "the length of time it took to successfully store mutations",
		Objectives: objectives,
	}, tableLabels)
	stageStoreErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "stage_store_errors_total",
		Help: "the number of times an error was encountered while storing mutations",
	}, tableLabels)
)

// Stagers returns a wrapper around the factory that adds
// performance-monitoring around the API.
func Stagers(s types.Stagers) types.Stagers {
	return &stagers{s}
}

type stagers struct {
	delegate types.Stagers
}

func (s *stagers) Get(ctx context.Context, target ident.Table) (types.Stager, error) {
	ret, err := s.delegate.Get(ctx, target)
	if ret != nil {
		ret = &stager{ret, tableValues(target)}
	}
	return ret, err
}

type stager struct {
	delegate types.Stager
	labels   []string
}

func (s *stager) Drain(
	ctx context.Context, tx pgxtype.Querier, prev, next hlc.Time,
) ([]types.Mutation, error) {
	start := time.Now()
	ret, err := s.delegate.Drain(ctx, tx, prev, next)
	d := time.Since(start)
	if err == nil {
		stageDrainCount.WithLabelValues(s.labels...).Add(float64(len(ret)))
		stageDrainDurations.WithLabelValues(s.labels...).Observe(d.Seconds())
	} else {
		stageDrainErrors.WithLabelValues(s.labels...).Inc()
	}
	return ret, err
}

func (s *stager) Store(ctx context.Context, db types.Batcher, muts []types.Mutation) error {
	start := time.Now()
	err := s.delegate.Store(ctx, db, muts)
	d := time.Since(start)
	if err == nil {
		stageStoreCount.WithLabelValues(s.labels...).Add(float64(len(muts)))
		stageStoreDurations.WithLabelValues(s.labels...).Observe(d.Seconds())
	} else {
		stageStoreErrors.WithLabelValues(s.labels...).Inc()
	}
	return err
}
