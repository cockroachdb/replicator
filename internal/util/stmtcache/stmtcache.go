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

// Package stmtcache provides a cache for prepared statements.
package stmtcache

import (
	"context"
	"database/sql"
	"sync"

	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/golang/groupcache/lru"
	"github.com/pkg/errors"
)

// stmtAdopter is implemented by *sql.Tx. We'll prepare the statements
// against the pool and, if necessary, bind the statement to a specific
// transaction for execution.
type stmtAdopter interface {
	StmtContext(context.Context, *sql.Stmt) *sql.Stmt
}

// Cache holds prepared statements which are retrieved by a comparable
// key. Since prepared statements may have a non-trivial cost or be a
// limited resource in the target database, so we want the Cache to have
// a one-to-one lifetime with the underlying database pool.
type Cache[T comparable] struct {
	db      *sql.DB
	toClose chan *sql.Stmt

	mu struct {
		sync.Mutex // Not RW since the LRU list moves elements.
		cache      *lru.Cache
	}
}

// New constructs a Cache for the pool.
func New[T comparable](ctx *stopper.Context, db *sql.DB, size int) *Cache[T] {
	ret := &Cache[T]{
		db:      db,
		toClose: make(chan *sql.Stmt, size),
	}
	ret.mu.cache = lru.New(size)
	ret.mu.cache.OnEvicted = func(_ lru.Key, value interface{}) {
		select {
		case ret.toClose <- value.(*sql.Stmt):
		default:
			// Not great, but we don't want to block. Anything we might
			// leak will be cleared when the relevant connection ages
			// out of the pool.
			stmtCacheDrops.Inc()
		}
	}
	ctx.Go(func() error {
		for {
			select {
			case stmt := <-ret.toClose:
				_ = stmt.Close()
				stmtCacheReleases.Inc()
			case <-ctx.Stopping():
				return nil
			}
		}
	})
	return ret
}

// Diagnostic implements [diag.Diagnostic]. It returns the capacity and
// size of the cache.
func (c *Cache[T]) Diagnostic(_ context.Context) any {
	c.mu.Lock()
	defer c.mu.Unlock()
	return map[string]int{
		"cap": c.mu.cache.MaxEntries,
		"len": c.mu.cache.Len(),
	}
}

// Prepare returns or constructs a new prepared statement. If db is a
// [*sql.Tx], the statement will be attached to the transaction.
func (c *Cache[T]) Prepare(
	ctx context.Context, db any, key T, gen func() (string, error),
) (*sql.Stmt, error) {
	stmt, err := c.get(ctx, key, gen)
	if err != nil {
		return nil, err
	}
	if tx, ok := db.(stmtAdopter); ok {
		stmt = tx.StmtContext(ctx, stmt)
	}
	return stmt, nil
}

func (c *Cache[T]) get(ctx context.Context, key T, gen func() (string, error)) (*sql.Stmt, error) {
	// It's an LRU cache. Calling Get will alter memory.
	c.mu.Lock()
	found, ok := c.mu.cache.Get(key)
	c.mu.Unlock()
	if ok {
		stmtCacheHits.Inc()
		return found.(*sql.Stmt), nil
	}

	// We'll generate and prepare the statement outside a critical
	// region, since this can take an arbitrarily long amount of time.
	// If we prepare the same statement twice, that's OK.
	stmtCacheMisses.Inc()
	q, err := gen()
	if err != nil {
		return nil, err
	}

	stmt, err := c.db.PrepareContext(ctx, q)
	if err != nil {
		return nil, errors.Wrap(err, q)
	}

	c.mu.Lock()
	c.mu.cache.Add(key, stmt)
	c.mu.Unlock()
	return stmt, nil
}
