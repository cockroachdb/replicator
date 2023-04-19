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
	"math/rand"
	"runtime"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/google/uuid"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	defaultLifetime = time.Minute
	defaultPoll     = time.Second
	defaultRetry    = time.Second
)

// Config is passed to New.
type Config struct {
	Pool   pgxtype.Querier // Database access.
	Target ident.Table     // The lease table.

	// Guard provides a quiescent period between when a lease is
	// considered to be expired (i.e. a lease callback's context is
	// canceled) and its published expiration time. This is useful for
	// situations where it may not be possible to immediately cancel all
	// side effects of a lease callback (e.g. it makes an external
	// network request).
	Guard time.Duration

	Lifetime   time.Duration // Duration of the lease.
	Poll       time.Duration // How often to re-check for an available lease.
	RetryDelay time.Duration // Delay between re-executing a callback.
}

// sanitize checks for mis-configuration and applies sane defaults.
func (c *Config) sanitize() error {
	if c.Pool == nil {
		return errors.New("pool must not be nil")
	}
	if c.Target == (ident.Table{}) {
		return errors.New("target must be set")
	}
	// OK for Guard to be zero.
	if c.Lifetime == 0 {
		c.Lifetime = defaultLifetime
	}
	if c.Poll == 0 {
		c.Poll = defaultPoll
	}
	if c.RetryDelay == 0 {
		c.RetryDelay = defaultRetry
	}
	return nil
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

// leaseFacade implements the public types.Lease interface.
type leaseFacade struct {
	cancel func()
	ctx    context.Context
}

var _ types.Lease = (*leaseFacade)(nil)

// Context implements types.Lease.
func (f *leaseFacade) Context() context.Context {
	return f.ctx
}

// Release implements types.Lease.
func (f *leaseFacade) Release() {
	f.cancel()
}

var _ types.Leases = (*leases)(nil)

const (
	schema = `
CREATE TABLE IF NOT EXISTS %s (
  name STRING PRIMARY KEY,
  expires TIMESTAMP NOT NULL,
  nonce UUID NOT NULL
)`
)

// New constructs an instance of types.Leases.
func New(ctx context.Context, cfg Config) (types.Leases, error) {
	if err := cfg.sanitize(); err != nil {
		return nil, err
	}
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

// Acquire the named lease, keep it alive, and return a facade.
func (l *leases) Acquire(ctx context.Context, name string) (types.Lease, error) {
	leaseRow, ok, err := l.acquire(ctx, name)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, &types.LeaseBusyError{Expiration: leaseRow.expires}
	}

	ctx, cancel := context.WithCancel(ctx)
	go func() {
		l.keepRenewed(ctx, leaseRow)
		cancel()
	}()

	ret := &leaseFacade{
		cancel: func() {
			_, _ = l.release(ctx, leaseRow)
			cancel()
		},
		ctx: ctx,
	}

	runtime.SetFinalizer(ret, func(f *leaseFacade) { f.Release() })

	return ret, nil
}

// Singleton executes a callback when the named lease is acquired.
//
// The lease will be released in the following circumstances:
//   - The callback returns nil.
//   - The lease cannot be renewed before it expires.
//   - The outer context is canceled.
//
// If the callback returns a non-nil error, the error will be logged. If
// the callback returns ErrCancelSingleton, it will not be retried. In
// all other cases, the callback function is retried once a lease is
// re-acquired.
func (l *leases) Singleton(ctx context.Context, name string, fn func(ctx context.Context) error) {
	// It's easier to ensure cleanup behavior using defer keyword. This
	// function returns false when the singleton should be torn down.
	loopBehavior := func() bool {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		tgt, ok := l.waitToAcquire(ctx, name)
		if !ok {
			// Context cancellation.
			return false
		}

		// Try to clean up, if possible. This uses a background context
		// so that the release code will still run if the outer context
		// was cancelled.
		defer func() {
			_, _ = l.release(context.Background(), tgt)
		}()

		// Create a channel to delay the return from this function until
		// we know that the renewal goroutine has shut down.
		renewStopped := make(chan struct{})
		go func() {
			l.keepRenewed(ctx, tgt)
			cancel()
			close(renewStopped)
		}()

		// Execute the callback.
		err := fn(ctx)

		// Ensure the renewal goroutine has stopped.
		cancel()
		<-renewStopped

		if errors.Is(err, types.ErrCancelSingleton) || errors.Is(err, context.Canceled) {
			log.WithField("lease", name).Trace("callback requested shutdown or was canceled")
			return false
		}
		log.WithField("lease", name).WithError(err).Error("lease callback exited; continuing")
		return true
	}

	for loopBehavior() {
		time.Sleep(l.cfg.RetryDelay)
	}
}

// waitToAcquire blocks until the named lease can be acquired. If the
// context is canceled, this method will return nil.
func (l *leases) waitToAcquire(ctx context.Context, name string) (acquired lease, ok bool) {
	entry := log.WithField("lease", name)

	// The zero value means the first read to the timer channel will
	// return immediately.
	t := time.NewTimer(0)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			entry.Trace("context canceled before acquisition")
			return lease{}, false
		case <-t.C:
		}

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

		// We want to poll, rather than to wait for the known lease to
		// expire to detect a deleted lease sooner. The random delay
		// helps to smear the requests over time. The expected value of
		// the rand call is Poll/2, so we add to only half of the
		// polling interval to hit the desired rate.
		pollDelay := l.cfg.Poll/2 + time.Duration(rand.Int63n(int64(l.cfg.Poll)))
		t.Reset(pollDelay)
	}
}

// keepRenewed will return when the lease cannot be renewed or when the
// context is canceled. It returns the status of the lease at the last
// successful renewal to aid in testing.
func (l *leases) keepRenewed(ctx context.Context, tgt lease) lease {
	for {
		now := time.Now().UTC()
		remaining := tgt.expires.Sub(now)
		// We haven't been able to renew before hitting the guard
		// duration, so return and allow the lease to be canceled.
		if remaining < l.cfg.Guard {
			return tgt
		}
		// Wait up to half of the remaining validity time before
		// attempting to renew, but rate-limit to the polling interval.
		delay := remaining / 2
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
		case errors.Is(err, context.Canceled):
			entry.Trace("context canceled")
			return tgt
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
func (l *leases) acquire(
	ctx context.Context, name string,
) (leaseRow lease, acquired bool, err error) {
	err = retry.Retry(ctx, func(ctx context.Context) error {
		var err error
		leaseRow, acquired, err = l.tryAcquire(ctx, name, time.Now())
		return err
	})
	return
}

// SQL template to claim a lease
//
//	$1 = name
//	$2 = caller-assigned expiration
//	$3 = caller-assigned now(), to ease testing
//
// Returns a nonce value if the lease was acquired.
//
// If needed, this could be extended to support atomic acquisition of
// multiple names by making $1 an array and unnest().
const acquireTemplate = `
WITH
  proposed (name, expires, nonce) AS (VALUES ($1::STRING, $2::TIMESTAMP, gen_random_uuid())),
  blocking AS (
    SELECT x.expires
    FROM %[1]s x
    JOIN proposed USING (name)
    WHERE x.expires > $3::TIMESTAMP
    FOR UPDATE
    LIMIT 1),
  acquired AS (
    UPSERT INTO %[1]s
    SELECT name, expires, nonce
    FROM proposed
    WHERE (SELECT count(*) FROM blocking) = 0
    RETURNING nonce)
SELECT (SELECT expires FROM blocking), (SELECT nonce FROM acquired)
`

// tryAcquire returns the current state of the named lease in the
// database. The ok value will be true if this call acquired the lease.
func (l *leases) tryAcquire(
	ctx context.Context, name string, now time.Time,
) (leaseRow lease, acquired bool, err error) {
	// We only have millisecond-level resolution in the db.
	now = now.UTC().Round(time.Millisecond)
	expires := now.Add(l.cfg.Lifetime)
	var blockedUntil *time.Time
	var nonce uuid.UUID

	// Explicit call to Format needed for compatibility with CRDB 20.2.
	if err := l.cfg.Pool.QueryRow(ctx,
		l.sql.acquire,
		name,
		expires.Format(time.RFC3339Nano),
		now.Format(time.RFC3339Nano),
	).Scan(&blockedUntil, &nonce); err != nil {
		return lease{}, false, errors.WithStack(err)
	}
	if blockedUntil != nil {
		return lease{*blockedUntil, name, uuid.UUID{}}, false, nil
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
//
//	$1 = name
//	$2 = nonce previously allocated by the database
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
//
//	$1 = new expiration time
//	$2 = name
//	$3 = nonce
const renewTemplate = `UPDATE %s SET expires=$1::TIMESTAMP WHERE name=$2::STRING AND nonce=$3::UUID`

// tryRenew updates the lease record in the database. If successful, the
// input lease struct will be updated with the new expiration time and
// returned to the caller. The boolean return value will be false if the
// lease was stolen.
func (l *leases) tryRenew(ctx context.Context, tgt lease, now time.Time) (lease, bool, error) {
	now = now.UTC()
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
