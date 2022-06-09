// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package leases

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/target/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

func TestLeases(t *testing.T) {
	a := assert.New(t)
	ctx, dbInfo, cancel := sinktest.Context()
	defer cancel()

	dbName, cancel, err := sinktest.CreateDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	tbl, err := sinktest.CreateTable(ctx, dbName, schema)
	if !a.NoError(err) {
		return
	}

	intf, err := New(ctx, Config{
		Pool:   dbInfo.Pool(),
		Target: tbl.Name(),
	})
	l := intf.(*leases)
	if !a.NoError(err) {
		return
	}

	now := time.Now().UTC()

	t.Run("tryAcquire", func(t *testing.T) {
		a := assert.New(t)

		// No present state, this should succeed.
		initial, ok, err := l.tryAcquire(ctx, t.Name(), now)
		a.NoError(err)
		a.True(ok)
		a.NotZero(initial)

		// Acquiring at the same time should not do anything.
		_, ok, err = l.tryAcquire(ctx, t.Name(), now)
		a.NoError(err)
		a.False(ok)

		// Acquire within the validity period should be a no-op.
		_, ok, err = l.tryAcquire(ctx, t.Name(), now.Add(l.cfg.Lifetime/2))
		a.NoError(err)
		a.False(ok)

		// Acquire at the expiration time should succeed.
		next, ok, err := l.tryAcquire(ctx, t.Name(), now.Add(l.cfg.Lifetime))
		a.NoError(err)
		if a.True(ok) {
			a.Equal(initial.name, next.name)
			a.NotEqual(initial.expires, next.expires)
			a.NotEqual(initial.nonce, next.nonce)
		}

		// Acquire within the extended lifetime should be a no-op.
		_, ok, err = l.tryAcquire(ctx, t.Name(), now.Add(2*l.cfg.Lifetime/3))
		a.NoError(err)
		a.False(ok)
	})

	t.Run("tryRelease", func(t *testing.T) {
		a := assert.New(t)

		initial, ok, err := l.tryAcquire(ctx, t.Name(), now)
		a.NoError(err)
		a.True(ok)
		a.NotZero(initial)

		// Verify that we can't release with a mis-matched nonce.
		l2 := initial
		l2.nonce = uuid.Must(uuid.NewRandom())
		ok, err = l.tryRelease(ctx, l2)
		a.NoError(err)
		a.False(ok)

		// Initial release should succeed.
		ok, err = l.tryRelease(ctx, initial)
		a.NoError(err)
		a.True(ok)

		// Duplicate release is a no-op.
		ok, err = l.tryRelease(ctx, initial)
		a.NoError(err)
		a.False(ok)
	})

	t.Run("tryRenew", func(t *testing.T) {
		a := assert.New(t)

		initial, ok, err := l.tryAcquire(ctx, t.Name(), now)
		a.NoError(err)
		a.True(ok)
		a.NotZero(initial)

		// Extend by a second.
		renewed, ok, err := l.tryRenew(ctx, initial, now.Add(time.Second))
		a.NoError(err)
		a.True(ok)
		a.Equal(now.Add(time.Second+l.cfg.Lifetime), renewed.expires)
		a.Equal(initial.name, renewed.name)
		a.Equal(initial.nonce, renewed.nonce)

		// Ensure that we can't cross-renew.
		mismatched := renewed
		mismatched.nonce = uuid.Must(uuid.NewRandom())
		_, ok, err = l.tryRenew(ctx, mismatched, now.Add(2*time.Second))
		a.NoError(err)
		a.False(ok)
	})

	t.Run("waitToAcquire", func(t *testing.T) {
		a := assert.New(t)

		// Increase polling rate for this test.
		oldCfg := l.cfg
		l.cfg.Poll = 10 * time.Millisecond
		defer func() { l.cfg = oldCfg }()

		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		initial, ok, err := l.acquire(ctx, t.Name())
		a.NoError(err)
		a.True(ok)
		a.NotZero(initial)

		eg, egCtx := errgroup.WithContext(ctx)
		eg.Go(func() error {
			acquired, ok := l.waitToAcquire(egCtx, initial.name)
			a.True(ok)
			a.NotZero(acquired)
			return nil
		})
		eg.Go(func() error {
			time.Sleep(l.cfg.Poll)
			ok, err := l.release(egCtx, initial)
			a.True(ok)
			a.NoError(err)
			return err
		})

		a.NoError(eg.Wait())
		// Make sure the context has not timed out.
		a.Nil(ctx.Err())
	})

	t.Run("keepRenewed", func(t *testing.T) {
		a := assert.New(t)

		// Increase polling rate and lower lifetime.
		oldCfg := l.cfg
		l.cfg.Lifetime = 100 * time.Millisecond
		l.cfg.Poll = 5 * time.Millisecond
		defer func() { l.cfg = oldCfg }()

		initial, ok, err := l.acquire(ctx, t.Name())
		a.NoError(err)
		a.True(ok)
		a.NotZero(initial)

		// Keep the
		renewCtx, cancel := context.WithTimeout(ctx, 3*l.cfg.Lifetime)
		defer cancel()
		final := l.keepRenewed(renewCtx, initial)

		a.Greater(final.expires, initial.expires)
	})

	// Verify that keepRenewed will return if the lease row in the
	// database is deleted from underneath.
	t.Run("keepRenewedExitsIfHijacked", func(t *testing.T) {
		a := assert.New(t)

		// Increase polling rate.
		oldCfg := l.cfg
		l.cfg.Lifetime = 100 * time.Millisecond
		l.cfg.Poll = 5 * time.Millisecond
		defer func() { l.cfg = oldCfg }()

		initial, ok, err := l.acquire(ctx, t.Name())
		a.NoError(err)
		a.True(ok)
		a.NotZero(initial)

		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		eg, egCtx := errgroup.WithContext(ctx)
		// Start a goroutine to keep the lease renewed, and a
		// second one to cause it to be released.
		eg.Go(func() error {
			l.keepRenewed(egCtx, initial)
			return nil
		})
		eg.Go(func() error {
			time.Sleep(l.cfg.Poll)
			ok, err := l.release(egCtx, initial)
			a.True(ok)
			a.NoError(err)
			return err
		})

		a.NoError(eg.Wait())
		// Make sure the context has not timed out.
		a.Nil(ctx.Err())
	})

	t.Run("singleton", func(t *testing.T) {
		a := assert.New(t)

		oldCfg := l.cfg
		// Ensure that cancel and cleanup are working; the lease
		// lifetime will be longer than that of the test.
		l.cfg.Lifetime = time.Hour
		// Increase polling rate.
		l.cfg.Poll = 5 * time.Millisecond
		defer func() { l.cfg = oldCfg }()

		ctx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()

		eg, egCtx := errgroup.WithContext(ctx)
		var running int32
		for i := 0; i < 100; i++ {
			eg.Go(func() error {
				// Each callback verifies that it's the only instance
				// running, then requests to be shut down.
				l.Singleton(egCtx, t.Name(), func(ctx context.Context) error {
					a.NoError(ctx.Err())
					if a.True(atomic.CompareAndSwapInt32(&running, 0, 1)) {
						time.Sleep(3 * l.cfg.Poll)
						atomic.StoreInt32(&running, 0)
					}
					return types.ErrCancelSingleton
				})
				return nil
			})
		}

		a.NoError(eg.Wait())
		// Make sure the context has not timed out.
		a.NoError(ctx.Err())
	})
}

func TestSanitize(t *testing.T) {
	a := assert.New(t)

	cfg := Config{}
	a.EqualError(cfg.sanitize(), "pool must not be nil")

	cfg.Pool = &pgxpool.Pool{}
	a.EqualError(cfg.sanitize(), "target must be set")

	cfg.Target = ident.NewTable(ident.New("db"), ident.Public, ident.New("tbl"))
	a.NoError(cfg.sanitize())

	a.Zero(cfg.Guard)
	a.Equal(defaultLifetime, cfg.Lifetime)
	a.Equal(defaultPoll, cfg.Poll)
	a.Equal(defaultRetry, cfg.RetryDelay)
}
