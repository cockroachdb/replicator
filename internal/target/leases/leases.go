// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package leases coordinates global, singleton activities.
package leases

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/google/uuid"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Config is passed to New.
type Config struct {
	Pool   pgxtype.Querier // Database access.
	Target ident.Table     // The lease table.

	// A buffer between the published expiration time of a lease and
	// when the callback's context will be canceled.
	GuardTime  time.Duration
	Lifetime   time.Duration // Duration of the lease.
	Poll       time.Duration // How often to re-check for an available lease.
	RetryDelay time.Duration // Delay between re-executing a callback.
}

type lease struct {
	expires time.Time
	name    string
	nonce   uuid.UUID
}

// leases coordinates global, singleton activities.
type leases struct {
	cfg Config
	sql struct {
		acquire string
		release string
		renew   string
	}
}

var _ types.Leases = (*leases)(nil)

const (
	schema = `
CREATE TABLE IF NOT EXISTS %s (
  name STRING PRIMARY KEY,
  expires TIMESTAMPTZ NOT NULL,
  nonce UUID NOT NULL
)`
)

// New constructs a instance of leases.
func New(ctx context.Context, cfg Config) (*leases, error) {
	_, err := cfg.Pool.Exec(ctx, fmt.Sprintf(schema, cfg.Target))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	l := &leases{cfg: cfg}
	l.sql.acquire = fmt.Sprintf(acquireTemplate, cfg.Target)
	l.sql.release = fmt.Sprintf(releaseTemplate, cfg.Target)
	l.sql.renew = fmt.Sprintf(renewTemplate, cfg.Target)

	return l, nil
}

// Singleton executes a callback when the named lease is acquired.
//
// The lease will be released in the following circumstances:
//   * The callback returns nil.
//   * The lease cannot be renewed before it expires.
//   * The outer context is canceled.
//
// If the callback returns a non-nil error, the error will be logged. If
// the callback returns ErrCancelSingleton, it will not be retried. In
// all other cases, the callback function is retried once a lease is
// re-acquired.
func (l *leases) Singleton(ctx context.Context, name string, fn func(ctx context.Context) error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// It's easier to ensure cleanup behavior using defer keyword.
	loopBehavior := func() {
		tgt, ok := l.waitToAcquire(ctx, name)
		if !ok {
			// Context cancellation.
			return
		}

		// Try to clean up, if possible. This uses a background context
		// so that the release code will still run if the outer context
		// was cancelled.
		defer func() {
			_, _ = l.release(context.Background(), tgt)
		}()

		// Start a goroutine to cancel the worker context.
		go func() {
			l.keepRenewed(ctx, tgt)
			cancel()
		}()

		// Execute the callback.
		err := fn(ctx)
		if errors.Is(err, types.ErrCancelSingleton) {
			log.WithField("lease", name).Trace("callback requested shutdown")
			cancel()
			return
		}
		log.WithField("lease", name).WithError(err).Error("lease callback exited; continuing")
	}

	for ctx.Err() == nil {
		loopBehavior()
	}
}

// waitToAcquire blocks until the named lease can be acquired. If the
// context is canceled, this method will return nil.
func (l *leases) waitToAcquire(ctx context.Context, name string) (acquired lease, ok bool) {
	entry := log.WithField("lease", name)
	for {
		entry.Trace("attempting to acquire")
		ret, ok, err := l.acquire(ctx, name)

		switch {
		case err != nil:
			entry.WithError(err).Warn("unable to acquire, will retry")
		case ok:
			entry.Trace("acquired")
			return ret, true
		default:
			entry.Trace("waiting")
		}

		select {
		case <-ctx.Done():
			entry.Trace("context canceled before acquisition")
			return lease{}, false
		case <-time.After(l.cfg.Poll):
			// Try again.
		}
	}
}

// keepRenewed will return with no error when the context is canceled.
// It returns the status of the lease at the last successful renewal to
// aid in testing.
func (l *leases) keepRenewed(ctx context.Context, tgt lease) lease {
	for {
		now := time.Now()
		// Wait up to half of the expiration time. Subtracting the
		// guard time may make the delay negative, so we expire.
		delay := tgt.expires.Sub(now)/2 - l.cfg.GuardTime
		if delay <= 0 {
			return tgt
		}
		// Rate-limit to polling interval.
		if delay < l.cfg.Poll {
			delay = l.cfg.Poll
		}

		// Wait until it's time to do something, or we're canceled.
		select {
		case <-ctx.Done():
			return tgt
		case <-time.After(delay):
		}

		var ok bool
		var err error
		tgt, ok, err = l.renew(ctx, tgt)

		entry := log.WithFields(log.Fields{
			"expires": tgt.expires, // Include renewed expiration time.
			"lease":   tgt.name,
		})

		switch {
		case err != nil:
			entry.WithError(err).Warn("could not renew lease")
			continue
		case !ok:
			entry.Debug("lease was hijacked")
			return tgt
		default:
			entry.Trace("renewed successfully")
		}
	}
}

// acquire returns a non-nil lease if it was able to acquire the named lease.
func (l *leases) acquire(ctx context.Context, name string) (acquired lease, ok bool, err error) {
	err = retry.Retry(ctx, func(ctx context.Context) error {
		var err error
		acquired, ok, err = l.tryAcquire(ctx, name, time.Now())
		return err
	})
	return
}

// SQL template to claim a lease
//   $1 = name
//   $2 = caller-assigned expiration
//   $3 = caller-assigned now(), to ease testing
// Returns a nonce value if the lease was acquired.
//
// If needed, this could be extended to support atomic acquisition of
// multiple names by making $1 an array and unnest().
const acquireTemplate = `
WITH
  proposed (name, expires, nonce) AS (VALUES ($1::STRING, $2::TIMESTAMPTZ, gen_random_uuid())),
  blocking AS (
    SELECT 1
    FROM %[1]s x
    JOIN proposed USING (name)
    WHERE x.expires > $3::TIMESTAMPTZ
    LIMIT 1)
UPSERT INTO %[1]s 
  SELECT name, expires, nonce
  FROM proposed
  WHERE (SELECT count(*) FROM blocking) = 0
  RETURNING nonce
`

// tryAcquire returns a non-nil lease if it was able to acquire the named lease.
func (l *leases) tryAcquire(
	ctx context.Context, name string, now time.Time,
) (acquired lease, ok bool, err error) {
	expires := now.Add(l.cfg.Lifetime)
	var nonce uuid.UUID

	row := l.cfg.Pool.QueryRow(ctx, l.sql.acquire, name, expires, now)
	if err := row.Scan(&nonce); errors.Is(err, pgx.ErrNoRows) {
		return lease{}, false, nil
	} else if err != nil {
		return lease{}, false, errors.WithStack(err)
	}

	return lease{expires, name, nonce}, true, nil
}

// release destroys the given lease.
func (l *leases) release(ctx context.Context, rel lease) (bool, error) {
	var ret bool
	err := retry.Retry(ctx, func(ctx context.Context) error {
		var err error
		ret, err = l.tryRelease(ctx, rel)
		return err
	})
	return ret, err
}

// SQL template for the current owner to release a lease.
//   $1 = name
//   $2 = nonce previously allocated by the database
const releaseTemplate = `DELETE FROM %s WHERE name=$1::STRING AND nonce=$2::UUID`

// tryRelease deletes the lease from the database.
func (l *leases) tryRelease(ctx context.Context, rel lease) (ok bool, err error) {
	tag, err := l.cfg.Pool.Exec(ctx, l.sql.release, rel.name, rel.nonce)
	if err != nil {
		return false, errors.WithStack(err)
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}
	rel.expires = time.Time{}
	rel.nonce = uuid.UUID{}
	return true, nil
}

func (l *leases) renew(ctx context.Context, tgt lease) (renewed lease, ok bool, err error) {
	err = retry.Retry(ctx, func(ctx context.Context) error {
		var err error
		renewed, ok, err = l.tryRenew(ctx, tgt, time.Now())
		return err
	})
	return
}

// SQL template to update the expiration time on a lease.
//   $1 = new expiration time
//   $2 = name
//   $3 = nonce
const renewTemplate = `UPDATE %s SET expires=$1::TIMESTAMPTZ WHERE name=$2::STRING AND nonce=$3::UUID`

// tryRenew updates the lease record in the database. If successful, the
// input lease struct will be updated with the new expiration time and
// returned to the caller. The boolean return value will be false if the
// lease was stolen.
func (l *leases) tryRenew(ctx context.Context, tgt lease, now time.Time) (lease, bool, error) {
	expires := now.Add(l.cfg.Lifetime)

	tag, err := l.cfg.Pool.Exec(ctx, l.sql.renew, expires, tgt.name, tgt.nonce)
	if err != nil {
		return tgt, false, errors.WithStack(err)
	}

	if tag.RowsAffected() == 0 {
		return tgt, false, nil
	}

	tgt.expires = expires
	return tgt, true, nil
}
