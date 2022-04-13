// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package memo implements a simple kv store with string keys and string value.
package memo

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/jackc/pgtype/pgxtype"
)

// Memo is a simple kv store with string keys and string values.
type Memo struct {
	pool pgxtype.Querier
	sql  struct {
		update  string
		restore string
	}
}

// NewMemo creates a structure to manage the key value store.
// It will create the table passed as an argument in the database, if it does not exists.
func NewMemo(ctx context.Context, tx pgxtype.Querier, target ident.Table) (*Memo, error) {
	if err := retry.Execute(ctx, tx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
key STRING NOT NULL,
value STRING NOT NULL,
PRIMARY KEY (key)
)
`, target)); err != nil {
		return nil, err
	}
	ret := &Memo{pool: tx}
	ret.sql.update = fmt.Sprintf(updateTemplate, target)
	ret.sql.restore = fmt.Sprintf(restoreTemplate, target)
	return ret, nil
}

const updateTemplate = `
UPSERT INTO %[1]s (key, value) VALUES ($1, $2)`

const restoreTemplate = `
SELECT value FROM %[1]s WHERE key = $1`

// Put stores the key-value in the target database
func (m *Memo) Put(ctx context.Context, key string, value string) error {
	return retry.Retry(ctx, func(ctx context.Context) error {
		_, err := m.pool.Exec(
			ctx,
			m.sql.update,
			key,
			value,
		)
		return err
	})

}

// Get retrieves a value given a key.
// If the key doesn't exists, it will return the default value supplied.
func (m *Memo) Get(ctx context.Context, key string, def string) (string, error) {
	s := def
	err := retry.Retry(ctx, func(ctx context.Context) error {
		res := m.pool.QueryRow(
			ctx,
			m.sql.restore,
			key,
		)
		res.Scan(&s)
		return nil
	})
	if err != nil {
		return def, err
	}
	return s, nil
}
