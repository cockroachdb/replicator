// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package txguard

import (
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/target/sinktest"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGuard(t *testing.T) {
	r := require.New(t)

	fixture, cancel, err := sinktest.NewBaseFixture()
	r.NoError(err)
	defer cancel()

	ctx := fixture.Context
	db := fixture.Pool

	const testPeriod = 10 * time.Millisecond

	t.Run("happy-path-commit", func(t *testing.T) {
		a := assert.New(t)
		r := require.New(t)

		tx, err := db.Begin(ctx)
		r.NoError(err)

		g := New(tx, WithPeriod(testPeriod), WithMaxMisses(10))
		a.NotNil(g.getTX())
		a.NoError(g.IsAlive())

		time.Sleep(2 * testPeriod)

		r.NoError(g.Commit(ctx))
		a.Nil(g.getTX())
		a.ErrorContains(g.IsAlive(), "transaction closed")

		// Validate use-after-done behavior.
		r.ErrorContains(g.Commit(ctx), "transaction not open")
		g.Rollback()
		r.ErrorContains(
			g.Use(func(pgxtype.Querier) error { return nil }),
			"transaction not open")
	})

	t.Run("happy-path-rollback", func(t *testing.T) {
		a := assert.New(t)
		r := require.New(t)

		tx, err := db.Begin(ctx)
		r.NoError(err)

		g := New(tx, WithPeriod(testPeriod), WithMaxMisses(10))
		a.NotNil(g.getTX())
		a.NoError(g.IsAlive())

		time.Sleep(2 * testPeriod)

		g.Rollback()
		a.Nil(g.getTX())
		a.ErrorContains(g.IsAlive(), "transaction closed")

		// Validate use-after-done behavior.
		r.ErrorContains(g.Commit(ctx), "transaction not open")
		g.Rollback()
		r.ErrorContains(
			g.Use(func(pgxtype.Querier) error { return nil }),
			"transaction not open")
	})

	t.Run("no-checkin", func(t *testing.T) {
		a := assert.New(t)
		r := require.New(t)

		tx, err := db.Begin(ctx)
		r.NoError(err)

		g := New(tx, WithPeriod(testPeriod), WithMaxMisses(1))
		time.Sleep(2 * testPeriod)
		a.ErrorContains(g.IsAlive(), "too many missed calls")
		a.Nil(g.getTX())
		r.ErrorContains(g.Commit(ctx), "keepalive previously failed")
		g.Rollback()
		r.ErrorContains(
			g.Use(func(pgxtype.Querier) error { return nil }),
			"keepalive previously failed")
	})

	t.Run("break-underlying-transaction", func(t *testing.T) {
		a := assert.New(t)
		r := require.New(t)

		tx, err := db.Begin(ctx)
		r.NoError(err)

		g := New(tx, WithPeriod(testPeriod), WithMaxMisses(10))
		a.NotNil(g.getTX())

		// Do a bad thing and kill the network connection.
		a.NoError(g.Use(func(tx pgxtype.Querier) error {
			return tx.(pgx.Tx).Conn().Close(ctx)
		}))

		time.Sleep(2 * testPeriod)

		// When the ping fails, it should clean up.
		a.Nil(g.getTX())
		a.ErrorContains(g.IsAlive(), "conn closed")

		// Validate use-after-fail behavior.
		r.ErrorContains(g.Commit(ctx), "keepalive previously failed")
		g.Rollback()
		r.ErrorContains(
			g.Use(func(pgxtype.Querier) error { return nil }),
			"keepalive previously failed")
	})

	t.Run("break-health-query", func(t *testing.T) {
		a := assert.New(t)
		r := require.New(t)

		tx, err := db.Begin(ctx)
		r.NoError(err)

		g := New(tx, WithPeriod(testPeriod), WithQuery("B0RK"))

		// Wait for the health-check query to error out.
		time.Sleep(2 * testPeriod)

		a.Nil(g.getTX())
		if pgErr := (*pgconn.PgError)(nil); a.ErrorAs(g.IsAlive(), &pgErr) {
			// Syntax error code
			a.Equal(pgErr.Code, "42601")
		}
	})
}
