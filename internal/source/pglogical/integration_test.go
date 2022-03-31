// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pglogical

// The tests in this package rely on the Docker Compose configurations
// in the .github directory.

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/target/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/util/batches"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4/pgxpool"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

var (
	pgConnString = flag.String(
		"pgConn",
		"postgres://postgres:SoupOrSecret@127.0.0.1/",
		"connection string for PostgreSQL instance")
)

// This is a general smoke-test of the logical replication feed.
func TestPGLogical(t *testing.T) {
	t.Run("consistent", func(t *testing.T) { testPGLogical(t, false) })
	t.Run("immediate", func(t *testing.T) { testPGLogical(t, true) })
}

func testPGLogical(t *testing.T, immediate bool) {
	a := assert.New(t)

	ctx, info, cancel := sinktest.Context()
	defer cancel()
	crdbPool := info.Pool()

	dbName, cancel, err := sinktest.CreateDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	pgPool, cancel, err := setupPGPool(dbName)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	// Create the schema in both locations.
	tgts := []ident.Table{
		ident.NewTable(dbName, ident.Public, ident.New("t1")),
		ident.NewTable(dbName, ident.Public, ident.New("t2")),
	}

	for _, tgt := range tgts {
		var schema = fmt.Sprintf(`CREATE TABLE %s (k INT PRIMARY KEY, v TEXT)`, tgt)
		if _, err := pgPool.Exec(ctx, schema); !a.NoError(err) {
			return
		}
		if _, err := crdbPool.Exec(ctx, schema); !a.NoError(err) {
			return
		}
	}

	// We want enough rows here to make sure that the batching and
	// coalescing logic gets exercised.
	rowCount := 10 * batches.Size()
	keys := make([]int, rowCount)
	vals := make([]string, rowCount)
	for i := range keys {
		keys[i] = i
		vals[i] = fmt.Sprintf("v=%d", i)
	}

	for _, tgt := range tgts {
		if _, err := pgPool.Exec(ctx,
			fmt.Sprintf("INSERT INTO %s VALUES (unnest($1::int[]), unnest($2::text[]))", tgt),
			keys, vals,
		); !a.NoError(err) {
			return
		}
	}

	// Start the connection, to demonstrate that we can backfill pending mutations.
	connCtx, cancelConn := context.WithCancel(ctx)
	defer cancelConn()
	_, stopped, err := NewConn(connCtx, &Config{
		ApplyTimeout: 2 * time.Minute, // Increase to make using the debugger easier.
		Immediate:    immediate,
		Publication:  dbName.Raw(),
		RetryDelay:   time.Nanosecond,
		Slot:         dbName.Raw(),
		SourceConn:   *pgConnString + dbName.Raw(),
		TargetConn:   crdbPool.Config().ConnString(),
		TargetDB:     dbName,
	})
	if !a.NoError(err) {
		return
	}

	// Wait for backfill.
	for _, tgt := range tgts {
		for {
			var count int
			if err := crdbPool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s", tgt)).Scan(&count); !a.NoError(err) {
				return
			}
			log.Trace("backfill count", count)
			if count == rowCount {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Let's perform an update in a single transaction.
	tx, err := pgPool.Begin(ctx)
	if !a.NoError(err) {
		return
	}
	for _, tgt := range tgts {
		if _, err := tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET v = 'updated'", tgt)); !a.NoError(err) {
			return
		}
	}
	if !a.NoError(tx.Commit(ctx)) {
		return
	}

	// Wait for the update to propagate.
	for _, tgt := range tgts {
		for {
			var count int
			if err := crdbPool.QueryRow(ctx,
				fmt.Sprintf("SELECT count(*) FROM %s WHERE v = 'updated'", tgt)).Scan(&count); !a.NoError(err) {
				return
			}
			log.Trace("update count", count)
			if count == rowCount {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Delete some rows.
	tx, err = pgPool.Begin(ctx)
	if !a.NoError(err) {
		return
	}
	for _, tgt := range tgts {
		if _, err := tx.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE k < 50", tgt)); !a.NoError(err) {
			return
		}
	}
	if !a.NoError(tx.Commit(ctx)) {
		return
	}

	// Wait for the deletes to propagate.
	for _, tgt := range tgts {
		for {
			var count int
			if err := crdbPool.QueryRow(ctx,
				fmt.Sprintf("SELECT count(*) FROM %s WHERE v = 'updated'", tgt)).Scan(&count); !a.NoError(err) {
				return
			}
			log.Trace("delete count", count)
			if count == rowCount-50 {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	cancelConn()
	select {
	case <-ctx.Done():
		a.Fail("cancelConn timed out")
	case <-stopped:
		// OK
	}
}

// This test creates a connection and sets various flags to ensure
// that we have covered certain failure cases.
func TestChaos(t *testing.T) {
	a := assert.New(t)

	ctx, info, cancel := sinktest.Context()
	defer cancel()
	crdbPool := info.Pool()

	dbName, cancel, err := sinktest.CreateDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	pgPool, cancel, err := setupPGPool(dbName)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	// Create the schema in both locations.
	tgt := ident.NewTable(dbName, ident.Public, ident.New("t"))
	var schema = fmt.Sprintf(`CREATE TABLE %s (k INT PRIMARY KEY, v TEXT)`, tgt)
	if _, err := pgPool.Exec(ctx, schema); !a.NoError(err) {
		return
	}
	if _, err := crdbPool.Exec(ctx, schema); !a.NoError(err) {
		return
	}

	// Start the connection.
	connCtx, cancelConn := context.WithCancel(ctx)
	defer cancelConn()
	_, stopped, err := NewConn(connCtx, &Config{
		Publication: dbName.Raw(),
		Slot:        dbName.Raw(),
		SourceConn:  *pgConnString + dbName.Raw(),
		RetryDelay:  time.Nanosecond,
		TargetConn:  crdbPool.Config().ConnString(),
		TargetDB:    dbName,
		TestControls: &TestControls{
			BreakReplicationFeed: func() bool {
				// We see multiple messages per row, so we don't need
				// to fail as often.
				return rand.Intn(100) == 0
			},
			BreakSinkFlush: func() bool {
				return rand.Intn(10) == 0
			},
			BreakOnDataTuple: func() bool {
				return rand.Intn(10) == 0
			},
		},
	})
	if !a.NoError(err) {
		return
	}

	// We're going to insert as a number of transactions, to ensure
	// that we cycle through all of the different error cases.
	const rowCount = 100
	for i := 0; i < rowCount; i++ {
		if _, err := pgPool.Exec(ctx,
			fmt.Sprintf("INSERT INTO %s VALUES ($1, $2)", tgt),
			i, fmt.Sprintf("v=%d", i),
		); !a.NoError(err) {
			return
		}
	}

	// Wait for everything to happen.
	for {
		var count int
		if err := crdbPool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s", tgt)).Scan(&count); !a.NoError(err) {
			return
		}
		if count == rowCount {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	cancelConn()
	select {
	case <-ctx.Done():
		a.Fail("cancelConn timed out")
	case <-stopped:
		// OK
	}
}

// https://www.postgresql.org/docs/current/datatype.html
func TestDataTypes(t *testing.T) {
	a := assert.New(t)

	// Build a long value for string and byte types.
	var sb strings.Builder
	for sb.Len() < 1<<16 {
		sb.WriteString("0123456789ABCDEF")
	}
	longString := sb.String()

	tcs := []struct {
		name   string
		values []interface{} // We automatically test for NULL below.
	}{
		{`bigint`, []interface{}{0, -1, 112358}},
		// bigserial doesn't exist as a reifiable type in pg?
		{`bit`, []interface{}{"10010101"}},
		{`bit varying`, []interface{}{"10010101"}},
		{`boolean`, []interface{}{true, false}},
		// box
		{`bytea`, []interface{}{"", "HELLO WORLD!", []byte(longString)}},
		{`character`, []interface{}{"", "HELLO WORLD!", longString}},
		{`character varying`, []interface{}{"", "HELLO WORLD!", longString}},
		// cidr not implemented: https://github.com/cockroachdb/cockroach/issues/18846
		// circle
		{`date`, []interface{}{"2020/02/02", time.Date(2022, 02, 22, 0, 0, 0, 0, time.UTC)}},
		{`double precision`, []interface{}{0.0, -1.1, 1.1}},
		{`inet`, []interface{}{"127.0.0.1"}},
		{`integer`, []interface{}{-1, 0, 112358}},
		{`interval`, []interface{}{time.Duration(0), time.Hour}},
		{`json`, []interface{}{"null", `{"hello":"world"}`, fmt.Sprintf(`{"long":%q}`, longString)}},
		{`jsonb`, []interface{}{"null", `{"hello":"world"}`, fmt.Sprintf(`{"long":%q}`, longString)}},
		// line
		// lseg
		// macaddr not implemented: https://github.com/cockroachdb/cockroach/issues/45813
		// money not implemented: https://github.com/cockroachdb/cockroach/issues/41578
		{`numeric`, []interface{}{"0", 55, 5.56}},
		// path
		// pg_lsn
		// pg_snapshot
		// point
		// polygon
		{`real`, []interface{}{0.0, -1.1, 1.1}},
		{`smallint`, []interface{}{0.0, -1, 1}},
		{`text`, []interface{}{``, `Hello World!`, longString}},
		{`time`, []interface{}{"16:20"}},
		{`time with time zone`, []interface{}{"16:20"}},
		// tsquery
		// tsvector
		// txid_snapshot
		{`uuid`, []interface{}{uuid.New()}},
		// xml
	}

	ctx, info, cancel := sinktest.Context()
	defer cancel()
	crdbPool := info.Pool()

	dbName, cancel, err := sinktest.CreateDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	pgPool, cancel, err := setupPGPool(dbName)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	// Create a dummy table for each type
	tgts := make([]ident.Table, len(tcs))
	for idx, tc := range tcs {
		tgt := ident.NewTable(dbName, ident.Public, ident.New(fmt.Sprintf("tgt_%s_%d", tc.name, idx)))
		tgts[idx] = tgt

		// Create the schema in both locations.
		var schema = fmt.Sprintf("CREATE TABLE %s (k INT PRIMARY KEY, v %s)",
			tgt, tc.name)
		if _, err := pgPool.Exec(ctx, schema); !a.NoErrorf(err, "PG %s", tc.name) {
			return
		}
		if _, err := crdbPool.Exec(ctx, schema); !a.NoErrorf(err, "CRDB %s", tc.name) {
			return
		}

		// Insert dummy data into the source in a single transaction.
		tx, err := pgPool.Begin(ctx)
		if !a.NoError(err) {
			return
		}

		for valIdx, value := range tc.values {
			if _, err := tx.Exec(ctx,
				fmt.Sprintf(`INSERT INTO %s VALUES ($1, $2::%s)`, tgt, tc.name), valIdx, value,
			); !a.NoErrorf(err, "%s %d %s", tc.name, valIdx, value) {
				return
			}
		}

		// Also insert a null value.
		if _, err := tx.Exec(ctx,
			fmt.Sprintf(`INSERT INTO %s VALUES ($1, NULL::%s)`, tgt, tc.name), -1,
		); !a.NoError(err) {
			return
		}

		a.NoError(tx.Commit(ctx))
	}
	log.Info(tgts)

	// Start the connection, to demonstrate that we can backfill pending mutations.
	connCtx, cancelConn := context.WithCancel(ctx)
	defer cancelConn()
	_, stopped, err := NewConn(connCtx, &Config{
		Publication: dbName.Raw(),
		RetryDelay:  time.Nanosecond,
		Slot:        dbName.Raw(),
		SourceConn:  *pgConnString + dbName.Raw(),
		TargetConn:  crdbPool.Config().ConnString(),
		TargetDB:    dbName,
	})
	if !a.NoError(err) {
		return
	}

	// Wait for rows to show up.
	for idx, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			a := assert.New(t)
			var count int
			for count == 0 {
				if err := crdbPool.QueryRow(ctx,
					fmt.Sprintf("SELECT count(*) FROM %s", tgts[idx])).Scan(&count); !a.NoError(err) {
					return
				}
				if count == 0 {
					time.Sleep(100 * time.Millisecond)
				}
			}
			// Expect the test data and a row with a NULL value.
			a.Equalf(len(tc.values)+1, count, "mismatch in %s", tgts[idx])
		})
	}

	cancelConn()
	select {
	case <-stopped:
	case <-ctx.Done():
	}
}

func setupPGPool(database ident.Ident) (*pgxpool.Pool, func(), error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	baseConn, err := pgxpool.Connect(ctx, *pgConnString)
	if err != nil {
		return nil, func() {}, err
	}

	if _, err := baseConn.Exec(ctx,
		fmt.Sprintf("CREATE DATABASE %s", database),
	); err != nil {
		return nil, func() {}, err
	}

	// Open the pool, using the newly-created database.
	next := baseConn.Config().Copy()
	next.ConnConfig.Database = database.Raw()
	retConn, err := pgxpool.ConnectConfig(ctx, next)
	if err != nil {
		return nil, func() {}, err
	}

	if _, err := retConn.Exec(ctx,
		fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", database),
	); err != nil {
		return nil, func() {}, err
	}

	if _, err := retConn.Exec(ctx,
		"SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
		database.Raw(),
	); err != nil {
		return nil, func() {}, err
	}

	return retConn, func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, err := retConn.Exec(ctx, "SELECT pg_drop_replication_slot($1)", database.Raw())
		if err != nil {
			log.WithError(err).Error("could not drop database")
		}
		_, err = retConn.Exec(ctx, fmt.Sprintf("DROP PUBLICATION %s", database))
		if err != nil {
			log.WithError(err).Error("could not drop publication")
		}
		retConn.Close()

		// Can't drop the default database from its own connection.
		_, err = baseConn.Exec(ctx, fmt.Sprintf("DROP DATABASE %s", database))
		if err != nil {
			log.WithError(err).Error("could not drop database")
		}
		baseConn.Close()
		log.Trace("finished pg pool cleanup")
	}, nil
}
