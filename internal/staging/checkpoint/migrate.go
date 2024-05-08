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

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/retry"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

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

		// No old table.
		if found == nil {
			return nil
		}

		constraint := ident.New(old.Table().Raw() + "_is_deprecated")

		// Determine if we've already run the migration by looking for
		// the check constraint. We need to fully-qualify the reference
		// to pg_catalog, since the connection has no default database
		// name.
		var count int
		if err := tx.QueryRow(ctx, fmt.Sprintf(`
SELECT count(*)
  FROM %s.pg_catalog.pg_constraint
 WHERE conrelid=$1::REGCLASS AND conname=$2`, old.Schema().Idents(nil)[0]),
			old.Raw(), constraint.Raw(),
		).Scan(&count); err != nil {
			return errors.WithStack(err)
		}

		// Already migrated.
		if count > 0 {
			return nil
		}

		log.Infof("migrating %s to %s", old, new)

		// Add a constraint that blocks all future writes.
		if _, err := tx.Exec(ctx, fmt.Sprintf(`
ALTER TABLE %s ADD CONSTRAINT %s CHECK ('this_table_is_deprecated'='') NOT VALID`,
			old, constraint),
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
