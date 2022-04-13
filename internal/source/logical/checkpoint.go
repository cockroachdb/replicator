// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logical

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	"github.com/jackc/pgtype/pgxtype"
)

// Checkpoint persist a checkpoint Stamp into a database table
// A checkpoint is specific for a particular source,
// identified by a key that should be universally unique
type Checkpoint struct {
	pool pgxtype.Querier
	key  string
	sql  struct {
		update  string
		restore string
	}
}

// NewCheckPoint creates a new checkpoint table, if it does not exists.
func NewCheckPoint(
	ctx context.Context, tx pgxtype.Querier, target ident.Table, key string,
) (*Checkpoint, error) {
	if err := retry.Execute(ctx, tx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
key STRING NOT NULL,
stamp STRING NOT NULL,
PRIMARY KEY (key)
)
`, target)); err != nil {
		return nil, err
	}
	ret := &Checkpoint{pool: tx, key: key}
	ret.sql.update = fmt.Sprintf(updateTemplate, target)
	ret.sql.restore = fmt.Sprintf(restoreTemplate, target)
	return ret, nil
}

const updateTemplate = `
UPSERT INTO %[1]s (key, stamp) VALUES ($1, $2)`

const restoreTemplate = `
SELECT stamp FROM %[1]s WHERE key = $1`

// Save stores a checkpoint (e.g. a stamp representing a transaction) in the target database
func (c *Checkpoint) Save(ctx context.Context, stamp stamp.Stamp) error {
	return retry.Retry(ctx, func(ctx context.Context) error {
		_, err := c.pool.Exec(
			ctx,
			c.sql.update,
			c.key,
			stamp.Marshall(),
		)
		return err
	})

}

// Restore retrieves the latest checkpoint.
// Typically used at start up to restart from a known good state.
func (c *Checkpoint) Restore(ctx context.Context, t stamp.Stamp) (stamp.Stamp, error) {
	s := t.Marshall()
	err := retry.Retry(ctx, func(ctx context.Context) error {
		res := c.pool.QueryRow(
			ctx,
			c.sql.restore,
			c.key,
		)
		res.Scan(&s)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return t.Unmarshall(s)
}
