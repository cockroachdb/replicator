// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cdc

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/target/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolverDeQueue(t *testing.T) {
	const rowCount = 100
	a := assert.New(t)
	r := require.New(t)

	baseFixture, cancel, err := sinktest.NewFixture()
	r.NoError(err)
	defer cancel()

	fixture, cancel, err := newTestFixture(baseFixture, &Config{
		MetaTableName: ident.New("resolved_timestamps"),
		BaseConfig: logical.BaseConfig{
			StagingDB:  baseFixture.StagingDB.Ident(),
			TargetConn: baseFixture.Pool.Config().ConnString(),
			TargetDB:   baseFixture.TestDB.Ident(),
		},
	})
	r.NoError(err)
	defer cancel()

	ctx := fixture.Context
	tbl, err := fixture.CreateTable(ctx,
		`CREATE TABLE %s (pk INT PRIMARY KEY, v INT NOT NULL)`)
	r.NoError(err)

	// Disable call to loop.Start().
	fixture.Resolvers.noStart = true
	resolver, err := fixture.Resolvers.get(ctx, tbl.Name().AsSchema())
	r.NoError(err)

	for i := int64(0); i < rowCount; i++ {
		r.NoError(resolver.Mark(ctx, hlc.New(i+1, 0)))
	}

	log.Info("marked")

	var committed hlc.Time
	for i := 0; i < rowCount; i++ {
		tx, err := fixture.Pool.Begin(ctx)
		r.NoError(err)

		found, err := resolver.dequeueInTx(ctx, tx, committed)
		// It's possible that the transaction commit below hasn't
		// finished its async cleanup by the time we roll around.
		// If this happens, try again.
		if err == errBlocked {
			i--
			_ = tx.Rollback(ctx)
			continue
		}
		r.NoError(err)
		a.Equal(int64(i), committed.Nanos()) // Verify expected order.

		// Verify that a concurrent transaction returns busy.
		_, err = resolver.dequeueInTx(ctx, fixture.Pool, committed)
		a.Equal(errBlocked, err)

		r.NoError(tx.Commit(ctx))
		committed = found
	}
	// Make sure we arrived at the end.
	a.Equal(hlc.New(rowCount, 0), committed)

	// Verify empty queue.
	_, err = resolver.dequeueInTx(ctx, fixture.Pool, committed)
	a.Equal(errNoWork, err)

	// Verify that we can enqueue a timestamp, even if there's an open
	// dequeue transaction running.
	heldOpen := hlc.New(rowCount+1, 0)
	r.NoError(resolver.Mark(ctx, heldOpen))
	for {
		tx, err := fixture.Pool.Begin(ctx)
		r.NoError(err)
		found, err := resolver.dequeueInTx(ctx, tx, heldOpen)
		// As above, it's possible that we're waiting for async cleanup.
		if err == errBlocked {
			_ = tx.Rollback(ctx)
			continue
		}
		r.NoError(err)
		a.Equal(heldOpen, found)
		timeout, cancel := context.WithTimeout(ctx, 5*time.Second)
		r.NoError(resolver.Mark(timeout, hlc.New(rowCount+2, 0)))
		cancel()
		r.NoError(tx.Commit(ctx))
		break
	}
}
