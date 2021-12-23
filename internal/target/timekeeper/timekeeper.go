// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package timekeeper implements a simple key-timestamp store.
package timekeeper

// The code in this file is adapted from resolved_table.go

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/metrics"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// timekeeper implements a simple key/value store for HLC timestamps.
type timekeeper struct {
	// A timekeeper is effectively a singleton, so we don't keep any
	// metrics in the instance.

	sql struct {
		swap string
	}
}

var _ types.TimeKeeper = (*timekeeper)(nil)

// NewTimeKeeper constructs a types.TimeKeeper using the specified table
// for storage.
func NewTimeKeeper(
	ctx context.Context, tx pgxtype.Querier, target ident.Table,
) (types.TimeKeeper, error) {
	if err := retry.Execute(ctx, tx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
key STRING NOT NULL PRIMARY KEY,
nanos INT8 NOT NULL,
logical INT8 NOT NULL
)
`, target)); err != nil {
		return nil, errors.WithStack(err)
	}

	ret := &timekeeper{}
	ret.sql.swap = fmt.Sprintf(swapTemplate, target)

	return ret, nil
}

const swapTemplate = `
WITH u AS (UPSERT INTO %[1]s (nanos, logical, key) VALUES ($1, $2, $3) RETURNING 0)
SELECT nanos, logical FROM %[1]s WHERE key=$3`

// Put updates the value associated with the key, returning the
// previous value.
func (s *timekeeper) Put(
	ctx context.Context, db pgxtype.Querier, schema ident.Schema, value hlc.Time,
) (hlc.Time, error) {
	var nanos int64
	var logical int
	start := time.Now()
	err := retry.Retry(ctx, func(ctx context.Context) error {
		return db.QueryRow(
			ctx,
			s.sql.swap,
			value.Nanos(),
			value.Logical(),
			schema.Raw()).Scan(&nanos, &logical)
	})

	// No rows means that we haven't seen this key before.
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		tkErrors.Inc()
		return hlc.Zero(), errors.Wrap(err, s.sql.swap)
	}
	ret := hlc.New(nanos, logical)

	d := time.Since(start)
	tkDurations.Observe(d.Seconds())
	labels := metrics.SchemaValues(schema)
	tkResolved.WithLabelValues(labels...).Set(float64(ret.Nanos()) / 1e9)
	log.WithFields(log.Fields{
		"target":   schema,
		"resolved": ret,
	}).Debug("put timestamp")
	return ret, nil
}
