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
	tkDurations = promauto.NewSummary(prometheus.SummaryOpts{
		Name:       "timekeeper_duration_seconds",
		Help:       "the length of time it took to successfully record resolved timestamps",
		Objectives: objectives,
	})
	tkErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "timekeeper_errors_total",
		Help: "the number of times an error was encountered while recording resolved timestamps",
	})
	tkResolved = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "timekeeper_schema_resolved_seconds",
		Help: "the last-known resolved timestamp that has been published for a database schema",
	}, schemaLabels)
)

// TimeKeeper returns a wrapper around the factory that adds
// performance-monitoring around the API.
func TimeKeeper(k types.TimeKeeper) types.TimeKeeper {
	return &timeKeeper{k}
}

type timeKeeper struct {
	k types.TimeKeeper
}

func (k *timeKeeper) Put(
	ctx context.Context, tx pgxtype.Querier, schema ident.Schema, ts hlc.Time,
) (hlc.Time, error) {
	start := time.Now()
	ret, err := k.k.Put(ctx, tx, schema, ts)
	d := time.Since(start)
	if err == nil {
		tkDurations.Observe(d.Seconds())

		resolved := time.Unix(0, ts.Nanos())
		tkResolved.WithLabelValues(schemaValues(schema)...).Set(float64(resolved.Unix()))
	} else {
		tkErrors.Inc()
	}
	return ret, err
}
