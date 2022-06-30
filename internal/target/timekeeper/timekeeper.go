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
	"flag"
	"sync"
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

	pool pgxtype.Querier
	mu   struct {
		sync.Mutex
		cleanups map[ident.Schema]hlc.Time
	}

	sql struct {
		append  string
		cleanup string
	}
}

var _ types.TimeKeeper = (*timekeeper)(nil)

// This query adds a new row at the end of the key's span. Since we
// include the timestamp data in the table's PK, we don't wind up
// overloading a single row with MVCC data that must be skipped over.
const appendTemplate = `
WITH u AS (UPSERT INTO %[1]s (nanos, logical, key) VALUES ($1, $2, $3) RETURNING 0)
SELECT nanos, logical FROM %[1]s WHERE key=$3 ORDER BY (nanos, logical) DESC LIMIT 1`

var cleanupDelay = flag.Duration("resolvedPurge", time.Second,
	"how often to purge unneeded resolved-timestamp entries; set to zero to disable")

// We execute this cleanup query on an occasional basis to retire
// unneeded rows. Since it has a bounded scan, it won't interfere with
// appends happening at the end of the key's span.
const cleanupTemplate = `
DELETE FROM %[1]s WHERE key=$3 AND (nanos, logical) < ($1, $2)
`

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
			s.sql.append,
			value.Nanos(),
			value.Logical(),
			schema.Raw()).Scan(&nanos, &logical)
	})

	// No rows means that we haven't seen this key before.
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		tkErrors.Inc()
		return hlc.Zero(), errors.Wrap(err, s.sql.append)
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

	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.cleanups[schema] = ret
	return ret, nil
}

func (s *timekeeper) cleanupLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(*cleanupDelay):
		}

		s.mu.Lock()
		old := s.mu.cleanups
		s.mu.cleanups = make(map[ident.Schema]hlc.Time)
		s.mu.Unlock()

		for schema, ts := range old {
			err := retry.Retry(ctx, func(ctx context.Context) error {
				tag, err := s.pool.Exec(ctx, s.sql.cleanup, ts.Nanos(), ts.Logical(), schema.Raw())
				log.Tracef("purged %d resolved timestamps from %s", tag.RowsAffected(), schema)
				return err
			})
			if err != nil {
				log.WithError(err).WithField("schema", schema).Warn("could not purge old resolved timestamps")
			}
		}
	}
}
