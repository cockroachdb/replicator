// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package feed

// This file contains the SQL persistence code for a feed configuration.

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
)

// loadFeed loads all feeds defined in the database and then triggers a
// refresh cycle.
func (f *Feeds) loadFeeds(ctx context.Context) error {
	// The implementation tries to be as "single-pass" as possible,
	// loading all interesting data from the three tables in single
	// shots.
	return retry.Retry(ctx, func(ctx context.Context) error {
		// Working map of names for correlating between the various queries.
		feeds := make(map[ident.Ident]*Feed)
		// Working map of target (db,schema,table) tuples to the
		// relevant Table objects. We guarantee that no two feeds have
		// overlapping table sets.
		tablesByTarget := make(map[ident.Table]*Table)

		tx, err := f.db.Begin(ctx)
		if err != nil {
			return errors.WithStack(err)
		}
		defer tx.Rollback(ctx)

		// Create the top-level Feed objects from the feeds table.
		rows, err := tx.Query(ctx, fmt.Sprintf(
			"SELECT feed_name, immediate, version, default_db, default_schema "+
				"FROM %s WHERE active",
			f.sql.feedsTable))
		if err != nil {
			return errors.WithStack(err)
		}
		defer rows.Close()

		var loaded []*Feed
		for rows.Next() {
			var defaultDB, defaultSchema ident.Ident
			feed := &Feed{Tables: map[ident.Table]*Table{}}
			if err := rows.Scan(
				&feed.Name, &feed.Immediate, &feed.Version, &defaultDB, &defaultSchema,
			); err != nil {
				return errors.WithStack(err)
			}
			feed.Schema = ident.NewSchema(defaultDB, defaultSchema)
			feeds[feed.Name] = feed
			loaded = append(loaded, feed)
		}

		// Load all tables in active feeds, and continue to fill
		// in the details of our in-memory objects.
		rows, err = tx.Query(ctx, fmt.Sprintf(
			"SELECT feed.feed_name, source_schema, source_table, "+
				"target_db, target_schema, target_table "+
				"FROM %s tbl "+
				"JOIN %s feed ON (tbl.feed_name = feed.feed_name AND feed.active)",
			f.sql.tableTable, f.sql.feedsTable,
		))
		if err != nil {
			return errors.WithStack(err)
		}
		defer rows.Close()
		for rows.Next() {
			var feedName, sourceSchema, sourceTable, targetDB, targetSchema, targetTable ident.Ident
			if err := rows.Scan(
				&feedName, &sourceSchema, &sourceTable, &targetDB, &targetSchema, &targetTable,
			); err != nil {
				return errors.WithStack(err)
			}

			feed, ok := feeds[feedName]
			if !ok {
				return errors.Errorf("did see expected feeds entry %q", feedName)
			}

			tbl := &Table{
				Target: ident.NewTable(targetDB, targetSchema, targetTable),
			}
			tbl.mu.columns = map[ident.Ident]*Column{}
			tablesByTarget[tbl.Target] = tbl
			feed.Tables[ident.NewTable(feed.Name, sourceSchema, sourceTable)] = tbl
		}

		// Load all columns in active feeds, correlating them with their
		// enclosing tables.
		rows, err = tx.Query(ctx, fmt.Sprintf(
			"SELECT feed_name, source_schema, source_table, source_column, "+
				"target_db, target_schema, target_table, "+
				"target_column, target_expr, cas_order, deadline, synthetic "+
				"FROM %s "+
				"JOIN %s USING (feed_name, source_schema, source_table) "+
				"JOIN %s USING (feed_name) "+
				"WHERE active",
			f.sql.columnsTable, f.sql.tableTable, f.sql.feedsTable,
		))
		if err != nil {
			return errors.WithStack(err)
		}
		defer rows.Close()
		for rows.Next() {
			var feedName, sourceSchema, sourceTable, sourceColumn ident.Ident
			var targetDB, targetSchema, targetTable, targetColumn ident.Ident
			var targetExpr string
			var casOrder int
			var deadline time.Duration
			var synthetic bool
			if err := rows.Scan(&feedName, &sourceSchema, &sourceTable, &sourceColumn,
				&targetDB, &targetSchema, &targetTable,
				&targetColumn, &targetExpr, &casOrder, &deadline, &synthetic); err != nil {
				return errors.WithStack(err)
			}

			col := &Column{
				CAS:        casOrder > 0,
				Deadline:   deadline,
				Expression: targetExpr,
				Target:     targetColumn,
				Synthetic:  synthetic,
			}
			tbl, ok := tablesByTarget[ident.NewTable(targetDB, targetSchema, targetTable)]
			if !ok {
				return errors.Errorf("did not find expected table %q.%q.%q", feedName, sourceSchema, sourceTable)
			}
			tbl.mu.columns[sourceColumn] = col

			// Place the column in the correct location within the CAS
			// slice. This may require growing the array until the
			// length is sufficient to add the value if we see a second
			// CAS column before the first. The casOrder value is a
			// 1-based index, so we can just use it as the minimum
			// length.
			if casOrder > 0 {
				for len(tbl.CAS) < casOrder {
					tbl.CAS = append(tbl.CAS, nil)
				}
				tbl.CAS[casOrder-1] = col
			}
		}

		f.mu.Lock()
		f.mu.active = loaded
		f.mu.Unlock()
		f.activeUpdated.Broadcast()
		configRefreshedAt.SetToCurrentTime()
		return nil
	})
}

// Store will persist the Feed into the database. If the feed is
// successfully stored, it will trigger a refresh to make the
// change visible to watchers.
func (f *Feeds) Store(ctx context.Context, feed *Feed) error {
	err := retry.Retry(ctx, func(ctx context.Context) error {
		if feed.Version == 0 {
			return errors.New("feed must have non-zero version")
		}

		tx, err := f.db.Begin(ctx)
		if err != nil {
			return errors.WithStack(err)
		}
		defer tx.Rollback(ctx)

		// Ensure persisted version only ever goes forward.
		var existingVersion int
		if err := tx.QueryRow(ctx, fmt.Sprintf(
			"SELECT version FROM %s WHERE feed_name = $1 FOR UPDATE",
			f.sql.feedsTable),
			feed.Name,
		).Scan(&existingVersion); err != nil {
			// No row is fine, first time inserting the feed.
			if !errors.Is(err, pgx.ErrNoRows) {
				return errors.WithStack(err)
			}
		}
		if feed.Version <= existingVersion {
			return errors.Errorf("version %d is not greater than persisted version %d",
				feed.Version, existingVersion)
		}

		// Store the new data.
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			"UPSERT INTO %s (feed_name, immediate, version, default_db, default_schema) "+
				"VALUES ($1, $2, $3, $4, $5)",
			f.sql.feedsTable),
			feed.Name,
			feed.Immediate,
			feed.Version,
			feed.Schema.Database(),
			feed.Schema.Schema(),
		); err != nil {
			return errors.WithStack(err)
		}

		// Remove anything not in the current configuration.
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			"DELETE FROM %s WHERE feed_name = $1",
			f.sql.tableTable),
			feed.Name,
		); err != nil {
			return errors.WithStack(err)
		}

		// Insert all table data.
		for sourceID, tbl := range feed.Tables {
			if _, err := tx.Exec(ctx, fmt.Sprintf(
				"INSERT INTO %s ("+
					"feed_name, source_schema, source_table, target_db, target_schema, target_table"+
					") VALUES ($1, $2, $3, $4, $5, $6)",
				f.sql.tableTable),
				feed.Name,
				sourceID.Schema(),
				sourceID.Table(),
				tbl.Target.Database(),
				tbl.Target.Schema(),
				tbl.Target.Table(),
			); err != nil {
				return errors.WithStack(err)
			}

			casOrders := make(map[*Column]int)
			for idx, casCol := range tbl.CAS {
				casOrders[casCol] = idx
			}

			// Upsert only interesting columns, delete the rest.
			if _, err := tx.Exec(ctx, fmt.Sprintf(
				"DELETE FROM %s WHERE feed_name=$1 AND source_schema=$2 AND source_table=$3",
				f.sql.columnsTable),
				feed.Name,
				sourceID.Schema(),
				sourceID.Table()); err != nil {
				return errors.WithStack(err)
			}

			for columnSourceID, col := range tbl.mu.columns {
				if col.marshalPayload(columnSourceID).IsZero() {
					continue
				}

				var casOrder int
				if idx, ok := casOrders[col]; ok {
					casOrder = idx + 1
				}

				if _, err := tx.Exec(ctx, fmt.Sprintf(
					"UPSERT INTO %s ("+
						"feed_name, source_schema, source_table, source_column, target_column, "+
						"target_expr, cas_order, deadline, synthetic"+
						") VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
					f.sql.columnsTable),
					feed.Name,
					sourceID.Schema(),
					sourceID.Table(),
					columnSourceID,
					col.Target,
					col.Expression,
					casOrder,
					col.Deadline,
					col.Synthetic,
				); err != nil {
					return errors.WithStack(err)
				}
			}
		}

		return tx.Commit(ctx)
	})
	if err != nil {
		return err
	}
	// Trigger immediate refresh before Store returns.
	return f.loadFeeds(ctx)
}
