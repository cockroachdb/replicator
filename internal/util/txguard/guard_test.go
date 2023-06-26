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

package txguard

import (
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/sinktest/base"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGuard(t *testing.T) {
	r := require.New(t)

	fixture, cancel, err := base.NewFixture()
	r.NoError(err)
	defer cancel()

	ctx := fixture.Context
	db := fixture.StagingPool

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
			g.Use(func(types.StagingQuerier) error { return nil }),
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
			g.Use(func(types.StagingQuerier) error { return nil }),
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
			g.Use(func(types.StagingQuerier) error { return nil }),
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
		a.NoError(g.Use(func(tx types.StagingQuerier) error {
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
			g.Use(func(types.StagingQuerier) error { return nil }),
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
