// Copyright 2024 The Cockroach Authors
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

package checkpoint

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/google/wire"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Set is used by Wire.
var Set = wire.NewSet(ProvideCheckpoints)

// ProvideCheckpoints is called by Wire.
func ProvideCheckpoints(
	ctx context.Context, pool *types.StagingPool, meta ident.StagingSchema,
) (*Checkpoints, error) {
	metaTable := ident.NewTable(meta.Schema(), ident.New("checkpoints"))
	if _, err := pool.Exec(ctx, fmt.Sprintf(schema, metaTable)); err != nil {
		return nil, errors.WithStack(err)
	}

	// Perform migration from old resolved_timestamps schema.
	if err := migrate(ctx, pool,
		ident.NewTable(meta.Schema(), ident.New("resolved_timestamps")),
		metaTable); err != nil {
		return nil, err
	}

	return &Checkpoints{
		pool:      pool,
		metaTable: metaTable,
	}, nil
}

// migrate from the old resolved_timestamps table schema to the current
// checkpoints table schema. This function will add a check constraint
// to the resolved_timestamps table to prevent any future writes, rename
// the table, and copy data into the new checkpoints table.
func migrate(ctx context.Context, pool *types.StagingPool, old, new ident.Table) error {
	return retry.Retry(ctx, pool, func(ctx context.Context) error {
		tx, err := pool.Begin(ctx)
		if err != nil {
			return errors.WithStack(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()

		// Test for old table. This function accepts quoted idents.
		var found *string
		if err := tx.QueryRow(ctx, `select to_regclass($1)`, old.String()).Scan(&found); err != nil {
			return errors.WithStack(err)
		}
		if found == nil {
			return nil
		}
		log.Infof("migrating %s to %s", old, new)

		// Add a constraint that blocks all future writes.
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			`ALTER TABLE %s ADD CONSTRAINT %s CHECK ('this_table_is_deprecated'='') NOT VALID`,
			old, ident.New(old.Table().Raw()+"_deprecated")),
		); err != nil {
			return errors.WithStack(err)
		}

		// Rename it out of the way.
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			`ALTER TABLE %s RENAME TO %s`,
			old,
			ident.NewTable(old.Schema(), ident.New(old.Table().Raw()+"_old"))),
		); err != nil {
			return errors.WithStack(err)
		}

		if _, err := tx.Exec(ctx, fmt.Sprintf(`
INSERT INTO %s (group_name, partition, source_hlc, first_seen, target_applied_at)
SELECT target_schema, target_schema,
       source_nanos::DECIMAL + (source_logical::DECIMAL/1e10)::DECIMAL(10,10),
       first_seen, target_applied_at
  FROM %s
`, new, old),
		); err != nil {
			return errors.WithStack(err)
		}

		if err := tx.Commit(ctx); err != nil {
			return errors.WithStack(err)
		}

		log.Infof("migrated %s to %s", old, new)
		return nil
	})
}
