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

package mylogical

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/sinktest/all"
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

const loopName = "mylogicaltest"

type startStamp int

var _ stamp.Stamp = startStamp(0)

func (f startStamp) Less(other stamp.Stamp) bool {
	return f < other.(startStamp)
}
func (f startStamp) MarshalText() (text []byte, err error) {
	return []byte(strconv.FormatInt(int64(f), 10)), nil
}
func TestMain(m *testing.M) {
	all.IntegrationMain(m, all.MySQLName)
}
func TestMYLogical(t *testing.T) {
	t.Run("backfill", func(t *testing.T) { testMYLogical(t, true, false) })
	t.Run("consistent", func(t *testing.T) { testMYLogical(t, false, false) })
	t.Run("immediate", func(t *testing.T) { testMYLogical(t, false, true) })
}

func testMYLogical(t *testing.T, backfill, immediate bool) {
	a := assert.New(t)

	// Create a basic test fixture.
	fixture, cancel, err := all.NewFixture()
	if !a.NoError(err) {
		return
	}
	defer cancel()

	ctx := fixture.Context
	dbName := fixture.TargetSchema.Schema()
	crdbPool := fixture.TargetPool

	config := &Config{
		BaseConfig: logical.BaseConfig{
			ApplyTimeout:  2 * time.Minute, // Increase to make using the debugger easier.
			Immediate:     immediate,
			RetryDelay:    10 * time.Second,
			StagingSchema: fixture.StagingDB.Schema(),
			TargetConn:    crdbPool.ConnectionString,
		},
		LoopConfig: logical.LoopConfig{
			LoopName:     loopName,
			TargetSchema: dbName,
		},
		SourceConn: "mysql://root:SoupOrSecret@localhost:3306/mysql/?sslmode=disable",
		ProcessID:  123456,
	}
	if backfill {
		config.BackfillWindow = time.Minute
	}
	err = config.Preflight()
	if !a.NoError(err) {
		return
	}

	defer cancel()

	myPool, cancel, err := setupMYPool(config)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	// Create the schema in both locations.
	tgt := ident.NewTable(dbName, ident.New("t"))

	// MySQL only has a single-level namespace; that is, no user-defined schemas.
	if _, err := myExec(ctx, myPool,
		fmt.Sprintf(`CREATE TABLE %s (k INT PRIMARY KEY, v varchar(20))`, tgt.Table().Raw()),
	); !a.NoError(err) {
		log.Fatal(err)
		return
	}
	if _, err := crdbPool.ExecContext(ctx,
		fmt.Sprintf(`CREATE TABLE %s (k INT PRIMARY KEY, v string)`, tgt)); !a.NoError(err) {
		return
	}

	flavor, err := getFlavor(config)
	if !a.NoError(err) {
		return
	}

	gtidSet, err := loadInitialGTIDSet(ctx, flavor, myPool)
	config.DefaultConsistentPoint = gtidSet
	if !a.NoError(err) {
		return
	}

	// Insert data into source table.
	const rowCount = 1024
	if _, err := myDo(ctx, myPool,
		func(ctx context.Context, conn *client.Conn) (*mysql.Result, error) {
			if err := conn.Begin(); err != nil {
				return nil, err
			}
			defer conn.Rollback()

			for i := 0; i < rowCount; i++ {
				if _, err := conn.Execute(
					fmt.Sprintf("INSERT INTO %s VALUES (?, ?)", tgt.Table().Raw()),
					i, fmt.Sprintf("v=%d", i),
				); err != nil {
					return nil, err
				}
			}

			return nil, conn.Commit()
		},
	); !a.NoError(err) {
		return
	}

	// Start the connection, to demonstrate that we can backfill pending mutations.
	loop, cancelLoop, err := Start(ctx, config)
	if !a.NoError(err) {
		return
	}
	defer cancelLoop()

	for {
		var count int
		if err := crdbPool.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM %s", tgt)).Scan(&count); !a.NoError(err) {
			return
		}
		log.Trace("backfill count", count)
		if count == rowCount {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Let's perform an update in a single transaction.

	_, err = myDo(ctx, myPool,
		func(ctx context.Context, conn *client.Conn) (*mysql.Result, error) {
			if err := conn.Begin(); err != nil {
				return nil, err
			}
			defer conn.Rollback()
			conn.Execute(fmt.Sprintf("UPDATE %s SET v = 'updated'", tgt.Table().Raw()))
			return nil, conn.Commit()
		})

	if !a.NoError(err) {
		return
	}
	// Wait for the update to propagate.
	for {
		var count int
		if err := crdbPool.QueryRowContext(ctx,
			fmt.Sprintf("SELECT count(*) FROM %s WHERE v = 'updated'", tgt)).Scan(&count); !a.NoError(err) {
			return
		}
		log.Trace("update count", count)
		if count == rowCount {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	_, err = myDo(ctx, myPool,
		func(ctx context.Context, conn *client.Conn) (*mysql.Result, error) {
			if err := conn.Begin(); err != nil {
				return nil, err
			}
			defer conn.Rollback()
			conn.Execute(fmt.Sprintf("DELETE FROM %s WHERE k < 50", tgt.Table().Raw()))
			return nil, conn.Commit()
		})

	if !a.NoError(err) {
		return
	}

	// Wait for the deletes to propagate.

	for {
		var count int
		if err := crdbPool.QueryRowContext(ctx,
			fmt.Sprintf("SELECT count(*) FROM %s WHERE v = 'updated'", tgt)).Scan(&count); !a.NoError(err) {
			return
		}
		log.Trace("delete count", count)
		if count == rowCount-50 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	cancelLoop()
	select {
	case <-ctx.Done():
		a.Fail("cancelConn timed out")
	case <-loop.Stopped():
		// OK
	}
}

func TestDataTypes(t *testing.T) {
	a := assert.New(t)

	// Build a long value for string and byte types.
	var sb strings.Builder
	shortString := "0123456789ABCDEF"
	for sb.Len() < 1<<16 {
		sb.WriteString(shortString)
	}
	longString := sb.String()
	log.Debug(longString)
	tcs := []struct {
		mysql  string
		size   string
		crdb   string
		values []any // We automatically test for NULL below.
	}{
		// BIGINT(size)
		{`bigint`, ``, `bigint`, []any{0, -1, 112358}},
		// BINARY(size)
		{`binary`, `(128)`, `bytes`, []any{[]byte(shortString)}},
		// BIT(size)
		{`bit`, `(8)`, `varbit`, []any{10}},
		// BLOB(size)
		{`blob`, `(65536)`, `blob`, []any{[]byte(longString)}},
		// BOOL
		// BOOLEAN
		// Synonyms FOR TINYINT(1). The replication protocol will be encoded as MYSQL_TYPE_TINY.
		{`bool`, ``, `int`, []any{true, false}},
		{`boolean`, ``, `int`, []any{true, false}},
		// CHAR(size)
		{`char`, `(128)`, `string`, []any{[]byte(shortString)}},
		// DATE
		{`date`, ``, `date`, []any{"1000-01-01", "9999-12-31"}},
		// DATETIME(fsp)
		{`datetime`, ``, `timestamp`, []any{"1000-01-01 00:00:00", "9999-12-31 23:59:59"}},
		// DEC(size, d)
		// DECIMAL(size, d)
		{`dec`, `(11,10)`, `decimal`, []any{math.E, math.Phi, math.Pi}},
		{`decimal`, `(11,10)`, `decimal`, []any{math.E, math.Phi, math.Pi}},
		// DOUBLE PRECISION(size, d)
		// DOUBLE(size, d)
		{`double precision`, `(11,10)`, `decimal`, []any{math.E, math.Phi, math.Pi}},
		{`double`, `(11,10)`, `decimal`, []any{math.E, math.Phi, math.Pi}},
		// --------- TODO: ENUM(val1, val2, val3, ...)

		// FLOAT(p)
		// FLOAT(size, d)
		{`float`, `(11,10)`, `float`, []any{math.E, math.Phi, math.Pi}},
		// INT(size)
		// INTEGER(size)
		{`int`, ``, `int`, []any{0, -1, 112358}},
		{`integer`, `(16)`, `int`, []any{0, -1, 112358}},
		// JSON
		{`json`, ``, `json`, []any{`{"hello":"world"}`}},
		// LONGBLOB
		{`longblob`, ``, `blob`, []any{[]byte(longString)}},
		// LONGTEXT
		{`longtext`, ``, `text`, []any{longString}},
		// MEDIUMBLOB
		{`mediumblob`, ``, `blob`, []any{[]byte(longString)}},
		// MEDIUMINT(size)
		{`mediumint`, ``, `int`, []any{0, -1, 112358}},
		// MEDIUMTEXT
		{`mediumtext`, ``, `text`, []any{longString}},
		// --------- TODO: SET(val1, val2, val3, ...)

		// SMALLINT(size)
		{`smallint`, ``, `int`, []any{0, -1, 127}},

		// TEXT(size)
		{`text`, ``, `text`, []any{shortString}},
		// TIME(fsp)
		{`time`, ``, `time`, []any{"16:20:00"}},
		// TIMESTAMP(fsp)
		{`timestamp`, ``, `timestamp`, []any{"1970-01-01 00:00:01", "2038-01-19 03:14:07"}},
		// TINYBLOB
		{`tinyblob`, ``, `blob`, []any{[]byte(shortString)}},
		// TINYINT(size)
		{`tinyint`, ``, `int`, []any{0, -1, 1}},
		// TINYTEXT
		{`tinytext`, ``, `text`, []any{shortString}},
		// VARBINARY(size)
		{`varbinary`, `(128)`, `bytes`, []any{[]byte(shortString)}},
		// VARCHAR(size)
		{`varchar`, `(128)`, `text`, []any{shortString}},
		// YEAR
		{`year`, ``, `string`, []any{"2022"}},
	}

	// Create a basic test fixture.
	fixture, cancel, err := all.NewFixture()
	if !a.NoError(err) {
		return
	}
	defer cancel()

	ctx := fixture.Context
	dbName := fixture.TargetSchema.Schema()
	crdbPool := fixture.TargetPool

	config := &Config{
		BaseConfig: logical.BaseConfig{
			ApplyTimeout:  2 * time.Minute, // Increase to make using the debugger easier.
			Immediate:     false,           // we care about transaction semantics
			RetryDelay:    10 * time.Second,
			StagingSchema: fixture.StagingDB.Schema(),
			TargetConn:    crdbPool.ConnectionString,
		},
		LoopConfig: logical.LoopConfig{
			LoopName:     loopName,
			TargetSchema: dbName,
		},
		SourceConn: "mysql://root:SoupOrSecret@localhost:3306/mysql/?sslmode=disable",
		ProcessID:  123456,
	}
	err = config.Preflight()
	if !a.NoError(err) {
		return
	}

	myPool, cancel, err := setupMYPool(config)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	flavor, err := getFlavor(config)
	if !a.NoError(err) {
		return
	}
	gtidSet, err := loadInitialGTIDSet(ctx, flavor, myPool)
	config.DefaultConsistentPoint = gtidSet
	if !a.NoError(err) {
		return
	}

	// Create a dummy table for each type
	tgts := make([]ident.Table, len(tcs))
	for idx, tc := range tcs {
		tn := strings.ReplaceAll(tc.mysql, " ", "_")
		tgt := ident.NewTable(dbName, ident.New(fmt.Sprintf("tgt_%s_%d", tn, idx)))
		tgts[idx] = tgt

		// Create the schema in both locations.

		dt := tc.mysql + tc.size

		var schema = fmt.Sprintf("CREATE TABLE %s (k INT PRIMARY KEY, v %s)",
			tgt.Table().Raw(), dt)
		log.Trace(schema)
		if _, err := myExec(ctx, myPool, schema); !a.NoErrorf(err, "MySQL %s", tc.mysql) {
			return
		}
		schema = fmt.Sprintf("CREATE TABLE %s (k INT PRIMARY KEY, v %s)",
			tgt, tc.crdb)

		if _, err := crdbPool.ExecContext(ctx, schema); !a.NoErrorf(err, "CRDB %s", tc.crdb) {
			return
		}

		if _, err := myDo(ctx, myPool,
			func(ctx context.Context, conn *client.Conn) (*mysql.Result, error) {
				if err := conn.Begin(); err != nil {
					return nil, err
				}
				defer conn.Rollback()

				for valIdx, value := range tc.values {
					if _, err := conn.Execute(
						fmt.Sprintf(`INSERT INTO %s VALUES (?, ?)`, tgt.Table().Raw()), valIdx, value,
					); !a.NoErrorf(err, "%s %d %s", tgt.Table().Raw(), valIdx, value) {
						return nil, err
					}
				}
				// Also insert a null value.
				if _, err := conn.Execute(
					fmt.Sprintf(`INSERT INTO %s VALUES (?,  NULL)`, tgt.Table().Raw()), -1,
				); !a.NoError(err) {
					return nil, err
				}

				return nil, conn.Commit()
			},
		); !a.NoError(err) {
			return
		}
	}

	// Start the connection, to demonstrate that we can backfill pending mutations.
	loop, cancelLoop, err := Start(ctx, config)
	if !a.NoError(err) {
		return
	}
	defer cancelLoop()

	// Wait for rows to show up.
	for idx, tc := range tcs {
		t.Run(tc.mysql, func(t *testing.T) {
			a := assert.New(t)
			var count int
			for count == 0 {
				if err := crdbPool.QueryRowContext(ctx,
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

	cancelLoop()
	select {
	case <-loop.Stopped():
	case <-ctx.Done():
	}
}

func myDo(
	ctx context.Context,
	pool *client.Pool,
	fn func(context.Context, *client.Conn) (*mysql.Result, error),
) (*mysql.Result, error) {
	conn, err := pool.GetConn(ctx)
	if err != nil {
		return nil, err
	}
	defer pool.PutConn(conn)

	return fn(ctx, conn)
}

// myExec is similar in spirit to pgx.Exec.
func myExec(
	ctx context.Context, pool *client.Pool, stmt string, args ...any,
) (*mysql.Result, error) {
	return myDo(ctx, pool, func(ctx context.Context, conn *client.Conn) (*mysql.Result, error) {
		return conn.Execute(stmt, args...)
	})
}
func setupMYPool(config *Config) (*client.Pool, func(), error) {
	database := config.TargetSchema.Idents(nil)[0] // Extract database name.
	addr := fmt.Sprintf("%s:%d", config.host, config.port)
	var baseConn *client.Conn
	var err error
	for i := 0; i < 10; i++ {
		baseConn, err = client.Connect(addr, config.user, config.password, "")
		if err != nil {
			log.Warn(err)
			log.Warn("Failed to establish connection to MySQL. Retrying...")
			time.Sleep(2 * time.Second)
		} else {
			break
		}
	}
	if err != nil {
		return nil, func() {}, err
	}
	defer baseConn.Close()

	if _, err := baseConn.Execute(fmt.Sprintf("CREATE DATABASE %s", database.Raw())); err != nil {
		return nil, func() {}, err
	}

	if err := baseConn.UseDB(database.Raw()); err != nil {
		return nil, func() {}, err
	}

	pool := client.NewPool(
		log.WithField("mysql", true).Infof, // logger
		1,                                  // minSize
		1024,                               // maxSize
		10,                                 // maxIdle
		addr,                               // address
		config.user,                        // user
		config.password,                    // password
		database.Raw(),                     // default db
	)
	return pool, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		conn, err := pool.GetConn(ctx)
		if err != nil {
			log.WithError(err).Error("could not drop database")
			return
		}
		defer conn.Close()

		_, err = conn.Execute(fmt.Sprintf("DROP DATABASE %s", database.Raw()))
		if err != nil {
			log.WithError(err).Error("could not drop database")
		}
		log.Info("finished my pool cleanup")
	}, nil
}

// loadInitialGTIDSet connects to the source database to return a GTID
// that can be used as a reasonable starting point for replication.
func loadInitialGTIDSet(ctx context.Context, flavor string, myPool *client.Pool) (string, error) {
	var gtidSet string
	switch flavor {
	case mysql.MySQLFlavor:
		res, err := myExec(ctx, myPool, "select source_uuid, min(interval_start), max(interval_end) from mysql.gtid_executed group by source_uuid;")
		if err != nil {
			return "", err
		}
		var uuid string
		var last int64

		if len(res.Values) > 0 {
			uuid = string(res.Values[0][0].AsString())
			last = res.Values[0][2].AsInt64()
			log.Infof("Master status: %s %d", uuid, last)
		} else {
			return "", errors.New("Unable to retrieve master status")
		}
		gtidSet = fmt.Sprintf("%s:1-%d", uuid, last)
	case mysql.MariaDBFlavor:
		res, err := myExec(ctx, myPool, "select @@gtid_binlog_pos;")
		if err != nil {
			return "", err
		}
		if len(res.Values) > 0 {
			gtidSet = string(res.Values[0][0].AsString())
		}
	}

	log.Infof("gtidSet: %s", gtidSet)
	return gtidSet, nil
}
