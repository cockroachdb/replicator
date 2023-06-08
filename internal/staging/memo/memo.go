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

// Package memo implements a simple kv store with string keys.
package memo

import (
	"context"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
)

// Memo is a key store that persists a value associated to a key.
type Memo struct {
	sql struct {
		get    string
		update string
	}
}

var _ types.Memo = (*Memo)(nil)

const (
	schema = `
CREATE TABLE IF NOT EXISTS %[1]s (
  key   STRING NOT NULL PRIMARY KEY,
  value BYTES  NOT NULL
)`
	updateTemplate = `UPSERT INTO %[1]s (key, value) VALUES ($1, $2)`
	getTemplate    = `SELECT value FROM %[1]s WHERE key = $1`
)

// Get retrieves a value given a key or nil if it does not exist.
func (m *Memo) Get(ctx context.Context, tx types.Querier, key string) ([]byte, error) {
	var ret []byte
	err := retry.Retry(ctx, func(ctx context.Context) error {
		err := tx.QueryRow(
			ctx,
			m.sql.get,
			key,
		).Scan(&ret)
		return errors.WithStack(err)
	})
	if errors.Is(err, pgx.ErrNoRows) {
		err = nil
	}
	return ret, err
}

// Put stores the key-value in the target database
func (m *Memo) Put(ctx context.Context, tx types.Querier, key string, value []byte) error {
	return retry.Retry(ctx, func(ctx context.Context) error {
		_, err := tx.Exec(
			ctx,
			m.sql.update,
			key,
			value,
		)
		return err
	})

}
