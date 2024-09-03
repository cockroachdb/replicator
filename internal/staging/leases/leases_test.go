// Copyright 2023 The Cockroach Authors
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

package leases

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/replicator/internal/sinktest/base"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestLeases(t *testing.T) {
	r := require.New(t)

	fixture, err := base.NewFixture(t)
	r.NoError(err)

	ctx := fixture.Context

	enclosing, err := base.CreateSchema(ctx, fixture.StagingPool, "leases_")
	r.NoError(err)

	tbl, err := base.CreateTable(ctx, fixture.StagingPool, enclosing, schema)
	r.NoError(err)

	intf, err := New(ctx, Config{
		Pool:   fixture.StagingPool,
		Target: tbl.Name(),
	})
	r.NoError(err)
	l := intf.(*leases)

	now := time.Now().UTC()

	t.Run("tryAcquire", func(t *testing.T) {
		a := assert.New(t)
		names := []string{t.Name()}

		// No present state, this should succeed.
		initial, ok, err := l.tryAcquire(ctx, names, now)
		a.NoError(err)
		a.True(ok)
		a.NotZero(initial)

		// Acquiring at the same time should not do anything.
		blocked, ok, err := l.tryAcquire(ctx, names, now)
		a.NoError(err)
		a.False(ok)
		a.Equal(initial.expires, blocked.expires)

		// Acquire within the validity period should be a no-op.
		blocked, ok, err = l.tryAcquire(ctx, names, now.Add(l.cfg.Lifetime/2))
		a.NoError(err)
		a.False(ok)
		a.Equal(initial.expires, blocked.expires)

		// Acquire at the expiration time should succeed.
		next, ok, err := l.tryAcquire(ctx, names, now.Add(l.cfg.Lifetime))
		a.NoError(err)
		if a.True(ok) {
			a.Equal(initial.names, next.names)
			a.NotEqual(initial.expires, next.expires)
			a.NotEqual(initial.nonce, next.nonce)
		}

		// Acquire within the extended lifetime should be a no-op.
		blocked, ok, err = l.tryAcquire(ctx, names, now.Add(2*l.cfg.Lifetime/3))
		a.NoError(err)
		a.False(ok)
		a.Equal(next.expires, blocked.expires)
	})

	t.Run("tryAcquireSkew", func(t *testing.T) {
		a := assert.New(t)
		namesA := []string{t.Name(), "A"}
		namesB := []string{t.Name(), "B"}

		// No present state, this should succeed.
		initial, ok, err := l.tryAcquire(ctx, namesA, now)
		a.NoError(err)
		a.True(ok)
		a.NotZero(initial)

		// Acquiring at the same time should not do anything.
		blocked, ok, err := l.tryAcquire(ctx, namesA, now)
		a.NoError(err)
		a.False(ok)
		a.Equal(initial.expires, blocked.expires)

		// Ensure all-or-nothing behavior with multiple names.
		blocked, ok, err = l.tryAcquire(ctx, namesB, now)
		a.NoError(err)
		a.False(ok)
		a.Equal(initial.expires, blocked.expires)
	})

	t.Run("tryRelease", func(t *testing.T) {
		a := assert.New(t)
		names := []string{t.Name(), "foo"}

		initial, ok, err := l.tryAcquire(ctx, names, now)
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
		names := []string{t.Name()}

		initial, ok, err := l.tryAcquire(ctx, names, now)
		a.NoError(err)
		a.True(ok)
		a.NotZero(initial)

		// Extend by a second.
		renewed, ok, err := l.tryRenew(ctx, initial, now.Add(time.Second))
		a.NoError(err)
		a.True(ok)
		a.Equal(now.Add(time.Second+l.cfg.Lifetime), renewed.expires)
		a.Equal(initial.names, renewed.names)
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
		names := []string{t.Name()}

		// Increase polling rate for this test.
		l := l.copy()
		l.cfg.Poll = 10 * time.Millisecond

		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		initial, ok, err := l.acquire(ctx, names)
		a.NoError(err)
		a.True(ok)
		a.NotZero(initial)

		eg, egCtx := errgroup.WithContext(ctx)
		eg.Go(func() error {
			acquired, ok := l.waitToAcquire(egCtx, initial.names)
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
		names := []string{t.Name(), "more"}

		initial, ok, err := l.acquire(ctx, names)
		a.NoError(err)
		a.True(ok)
		a.NotZero(initial)

		// Time remaining on lease.
		renewed, retry := l.keepRenewedOnce(ctx, initial, initial.expires.Add(-time.Millisecond))
		a.True(retry)
		a.Greater(renewed.expires, initial.expires)

		// Context canceled
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		_, retry = l.keepRenewedOnce(canceled, initial, initial.expires.Add(-time.Millisecond))
		a.False(retry)

		// Already expired
		_, retry = l.keepRenewedOnce(ctx, initial, initial.expires.Add(time.Millisecond))
		a.False(retry)
	})

	// Verify that keepRenewed will return if any lease row in the
	// database is deleted from underneath.
	t.Run("keepRenewedExitsIfHijacked", func(t *testing.T) {
		a := assert.New(t)
		names := []string{t.Name(), "kaboom"}

		// Increase polling rate.
		l := l.copy()
		l.cfg.Lifetime = 100 * time.Millisecond
		l.cfg.Poll = 5 * time.Millisecond

		initial, ok, err := l.acquire(ctx, names)
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
		// Release only one of the names.
		eg.Go(func() error {
			time.Sleep(l.cfg.Poll)
			leaseCopy := initial
			leaseCopy.names = names[1:]
			ok, err := l.release(egCtx, leaseCopy)
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

		// Ensure that cancel and cleanup are working; the lease
		// lifetime will be longer than that of the test.
		l := l.copy()
		l.cfg.Lifetime = time.Hour
		l.cfg.Poll = 5 * time.Millisecond

		ctx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()

		eg, egCtx := errgroup.WithContext(ctx)
		var running atomic.Bool
		for i := 0; i < 10; i++ {
			eg.Go(func() error {
				// Each callback verifies that it's the only instance
				// running, then requests to be shut down.
				l.Singleton(egCtx, []string{t.Name()}, func(ctx context.Context) error {
					a.NoError(ctx.Err())
					if a.True(running.CompareAndSwap(false, true)) {
						time.Sleep(3 * l.cfg.Poll)
						running.Store(false)
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

	t.Run("lease_facade", func(t *testing.T) {
		a := assert.New(t)

		// Initial acquisition.
		facade, err := l.Acquire(ctx, t.Name())
		a.NoError(err)

		// Verify that a duplicate fails.
		_, err = l.Acquire(ctx, t.Name())
		if busy, ok := types.IsLeaseBusy(err); a.True(ok) {
			a.NotZero(busy.Expiration)
		}

		// Verify that releasing cancels the lease.
		a.Nil(facade.Context().Err())
		facade.Release()

		a.ErrorIs(facade.Context().Err(), context.Canceled)

		// Re-acquisition should succeed.
		_, err = l.Acquire(ctx, t.Name())
		a.NoError(err)
	})
}

func TestSanitize(t *testing.T) {
	a := assert.New(t)

	cfg := Config{}
	a.EqualError(cfg.sanitize(), "pool must not be nil")

	cfg.Pool = &types.StagingPool{}
	a.EqualError(cfg.sanitize(), "target must be set")

	cfg.Target = ident.NewTable(ident.MustSchema(ident.New("db"), ident.Public), ident.New("tbl"))
	a.NoError(cfg.sanitize())

	a.Zero(cfg.Guard)
	a.Equal(defaultLifetime, cfg.Lifetime)
	a.Equal(defaultPoll, cfg.Poll)
	a.Equal(defaultRetry, cfg.RetryDelay)
}
