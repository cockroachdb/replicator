// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package txguard provides a utility class that keeps a database
// transaction active on a periodic basis.
package txguard

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgtype/pgxtype"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Overridden by test code.
const (
	defaultMaxMisses = 5
	defaultPeriod    = time.Second
)

// Guard protects long-running database transactions in two ways.
// First, it will periodically execute a test statement on an existing
// database transaction to prevent an idle-in-session timeout from
// occurring. Secondly, it will roll the guarded transaction back if
// IsAlive is not called on a regular basis. The keepalive behavior will
// cease when either Commit or Rollback are called or if IsAlive is not
// called on a periodic basis.
type Guard struct {
	maxMisses int           // Maximum number of missed calls to IsAlive
	period    time.Duration // How ofter to execute the keepalive

	mu struct {
		sync.Mutex
		callerOK  bool   // A flag set by IsAlive
		error     error  // The exit cause for keepalive()
		missCount int    // The number of keepalive loops where callerOK was not set
		tx        pgx.Tx // The guarded transaction
	}
}

// New constructs a Guard around the given transaction. Once this
// function is called, the transaction should only ever be accessed via
// Guard.Use.
func New(tx pgx.Tx, options ...Option) *Guard {
	ret := &Guard{}
	ret.mu.tx = tx
	for _, o := range options {
		o.apply(ret)
	}
	if ret.maxMisses <= 0 {
		ret.maxMisses = defaultMaxMisses
	}
	if ret.period <= 0 {
		ret.period = defaultPeriod
	}
	ret.keepalive()
	return ret
}

// Commit the underlying transaction.
func (g *Guard) Commit(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if err := g.mu.error; err != nil {
		return errors.Wrap(err, "keepalive previously failed")
	}

	tx := g.mu.tx
	if tx == nil {
		return errors.New("transaction not open")
	}
	g.mu.tx = nil
	return errors.Wrap(tx.Commit(ctx), "could not commit tx")
}

// IsAlive returns an error if the transaction has been committed,
// rolled back, or if it experienced an error while keeping it alive.
func (g *Guard) IsAlive() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.mu.error; err != nil {
		return err
	}
	if g.mu.tx == nil {
		return errors.New("transaction closed")
	}
	g.mu.callerOK = true
	g.mu.missCount = 0
	return nil
}

// Rollback is safe to call multiple times.
func (g *Guard) Rollback() {
	g.mu.Lock()
	defer g.mu.Unlock()

	tx := g.mu.tx
	if tx == nil {
		return
	}
	g.mu.tx = nil
	_ = tx.Rollback(context.Background())
}

// Use accesses the underlying transaction in a thread-safe manner.
func (g *Guard) Use(fn func(tx pgxtype.Querier) error) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if err := g.mu.error; err != nil {
		return errors.Wrap(err, "keepalive previously failed")
	}

	tx := g.mu.tx
	if tx == nil {
		return errors.New("transaction not open")
	}
	return fn(tx)
}

// getTX is used by test code to safely access the mu.tx field.
func (g *Guard) getTX() pgx.Tx {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.mu.tx
}

// keepalive starts a background goroutine to occasionally ping
// the enclosed transaction. If an error is encountered, it will be made
// available from IsAlive.
func (g *Guard) keepalive() {
	tryPing := func(ctx context.Context) bool {
		g.mu.Lock()
		defer g.mu.Unlock()

		tx := g.mu.tx
		if tx == nil {
			return false
		}

		if g.mu.callerOK {
			g.mu.callerOK = false
			g.mu.missCount = 0
		} else {
			g.mu.missCount++
			if g.mu.missCount >= g.maxMisses {
				log.Debug("abandoned transaction")
				_ = tx.Rollback(ctx)
				g.mu.error = errors.New("too many missed calls")
				g.mu.tx = nil
				return false
			}
		}

		_, err := tx.Exec(ctx, "SELECT 1")
		if err == nil {
			return true
		}
		_ = tx.Rollback(ctx)
		g.mu.error = errors.Wrap(err, "could not ping tx")
		g.mu.tx = nil
		return false
	}

	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		for tryPing(ctx) {
			time.Sleep(g.period)
		}
	}()
}
