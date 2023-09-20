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

package apply

import (
	"context"
	"database/sql"
	"sync"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/batches"
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

// stmtCache wraps the templates generator to provide caching of
// prepared statements for our hot-path queries to a specific table. The
// cache will be discarded in response to schema changes.
type stmtCache struct {
	*templates // Allow read through to the underlying schema data.
	db         *types.TargetPool

	hits   prometheus.Counter
	misses prometheus.Counter

	mu struct {
		sync.Mutex // Not RW since the LRU list moves elements.

		cleared bool
		deletes *lru.Cache
		upserts *lru.Cache
	}
}

func newStmtCache(db *types.TargetPool, tmpls *templates) *stmtCache {
	labels := []string{tmpls.TableName.Schema().Raw(), tmpls.TableName.Table().Raw()}
	ret := &stmtCache{
		db:        db,
		hits:      stmtCacheHits.WithLabelValues(labels...),
		misses:    stmtCacheMisses.WithLabelValues(labels...),
		templates: tmpls,
	}
	ret.mu.deletes = lru.New(batches.Size())
	ret.mu.upserts = lru.New(batches.Size())

	ret.mu.deletes.OnEvicted = func(_ lru.Key, value interface{}) {
		_ = value.(*sql.Stmt).Close()
	}
	ret.mu.upserts.OnEvicted = ret.mu.deletes.OnEvicted

	return ret
}

// Delete returns a cached, prepared statement to delete the requested
// number of rows.
func (c *stmtCache) Delete(
	ctx context.Context, tx types.TargetQuerier, rowCount int,
) (*sql.Stmt, error) {
	stmt, err := c.get(ctx, c.mu.deletes, c.deleteExpr, rowCount)
	if err != nil {
		return nil, err
	}
	if a, ok := tx.(stmtAdopter); ok {
		stmt = a.StmtContext(ctx, stmt)
	}
	return stmt, nil
}

// Invalidate is called when the underlying schema information has
// changed and this cache has been replaced.
func (c *stmtCache) Invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.mu.cleared = true
	c.mu.deletes.Clear()
	c.mu.upserts.Clear()
}

// Upsert returns a cached, prepared statement to upsert the requested
// number of rows.
func (c *stmtCache) Upsert(
	ctx context.Context, tx types.TargetQuerier, rowCount int,
) (*sql.Stmt, error) {
	stmt, err := c.get(ctx, c.mu.upserts, c.upsertExpr, rowCount)
	if err != nil {
		return nil, err
	}
	if a, ok := tx.(stmtAdopter); ok {
		stmt = a.StmtContext(ctx, stmt)
	}
	return stmt, nil
}

// get will acquire a lock on the mutex before manipulating the cache.
func (c *stmtCache) get(
	ctx context.Context, cache *lru.Cache, gen func(int) (string, error), rowCount int,
) (*sql.Stmt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.mu.cleared {
		return nil, errors.New("stmtCache cleared")
	}

	if found, ok := cache.Get(rowCount); ok {
		c.hits.Inc()
		return found.(*sql.Stmt), nil
	}

	q, err := gen(rowCount)
	if err != nil {
		return nil, err
	}

	stmt, err := c.db.PrepareContext(ctx, q)
	if err != nil {
		return nil, errors.Wrap(err, q)
	}
	cache.Add(rowCount, stmt)
	c.misses.Inc()
	return stmt, nil
}
