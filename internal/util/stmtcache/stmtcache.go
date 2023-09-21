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

	"github.com/golang/groupcache/lru"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
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
	db *sql.DB

	hits   prometheus.Counter
	misses prometheus.Counter

	mu struct {
		sync.Mutex // Not RW since the LRU list moves elements.
		cache      *lru.Cache
	}
}

// New constructs a Cache for the pool.
func New[T comparable](db *sql.DB, size int) *Cache[T] {
	ret := &Cache[T]{
		db:     db,
		hits:   stmtCacheHits,
		misses: stmtCacheMisses,
	}
	ret.mu.cache = lru.New(size)
	ret.mu.cache.OnEvicted = func(_ lru.Key, value interface{}) {
		_ = value.(*sql.Stmt).Close()
	}

	return ret
}

// Close is called to remove and close all existing statements.
func (c *Cache[T]) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.mu.cache.Clear()
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
	c.mu.Lock()
	defer c.mu.Unlock()

	if found, ok := c.mu.cache.Get(key); ok {
		return found.(*sql.Stmt), nil
	}

	q, err := gen()
	if err != nil {
		return nil, err
	}

	stmt, err := c.db.PrepareContext(ctx, q)
	if err != nil {
		return nil, errors.Wrap(err, q)
	}
	c.mu.cache.Add(key, stmt)
	return stmt, nil
}
