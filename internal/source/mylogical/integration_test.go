// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package mylogical

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/target/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

type startStamp int

var _ stamp.Stamp = startStamp(0)

func (f startStamp) Less(other stamp.Stamp) bool {
	return f < other.(startStamp)
}
func (f startStamp) Marshall() string {
	return strconv.FormatInt(int64(f), 10)
}

func (f startStamp) Unmarshall(v string) (stamp.Stamp, error) {
	res, err := strconv.ParseInt(v, 0, 64)
	return startStamp(res), err
}
func TestMain(m *testing.M) {
	sinktest.IntegrationMain(m, sinktest.MySQLName)
}
func TestMYLogical(t *testing.T) {
	t.Run("consistent", func(t *testing.T) { testMYLogical(t, false) })
	t.Run("immediate", func(t *testing.T) { testMYLogical(t, true) })
}

func testMYLogical(t *testing.T, immediate bool) {
	a := assert.New(t)

	ctx, info, cancel := sinktest.Context()
	defer cancel()
	crdbPool := info.Pool()

	dbName, cancel, err := sinktest.CreateDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	myPool, cancel, err := setupMYPool(dbName)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	// Create the schema in both locations.
	tgt := ident.NewTable(dbName, ident.Public, ident.New("t"))

	// MySQL only has a single-level namespace; that is, no user-defined schemas.
	if _, err := myExec(ctx, myPool,
		fmt.Sprintf(`CREATE TABLE %s (k INT PRIMARY KEY, v varchar(20))`, tgt.Table().Raw()),
	); !a.NoError(err) {
		log.Fatal(err)
		return
	}
	if _, err := crdbPool.Exec(ctx,
		fmt.Sprintf(`CREATE TABLE %s (k INT PRIMARY KEY, v string)`, tgt)); !a.NoError(err) {
		return
	}

	res, err := myExec(ctx, myPool, "select source_uuid, min(interval_start), max(interval_end) from mysql.gtid_executed group by source_uuid;")
	if !a.NoError(err) {
		return
	}
	var uuid string
	var last int64

	if len(res.Values) > 0 {
		uuid = string(res.Values[0][0].AsString())
		last = res.Values[0][2].AsInt64()
		log.Infof("Master status: %s %d", uuid, last)
	} else {
		return
	}
	tbl := ident.NewTable(ident.StagingDB, ident.Public, ident.New("checkpoint"))
	cp, err := logical.NewCheckPoint(ctx, crdbPool, tbl, uuid)
	if !a.NoError(err) {
		return
	}

	cp.Save(ctx, startStamp(last))
	if err := cp.Save(ctx, startStamp(last)); !a.NoError(err) {
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
	connCtx, cancelConn := context.WithCancel(ctx)
	defer cancelConn()
	_, stopped, err := NewConn(connCtx, &Config{
		Config: logical.Config{
			ApplyTimeout:       2 * time.Minute, // Increase to make using the debugger easier.
			Immediate:          immediate,
			RetryDelay:         10 * time.Second,
			TargetConn:         crdbPool.Config().ConnString(),
			TargetDB:           dbName,
			CheckPointSourceID: uuid,
		},
		ServerID:   100,
		SourceConn: "mysql://root:SoupOrSecret@localhost:3306/mysql/?sslmode=disable",
	})
	if !a.NoError(err) {
		return
	}

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

	cancelConn()
	select {
	case <-ctx.Done():
		a.Fail("cancelConn timed out")
	case <-stopped:
		// OK
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
	ctx context.Context, pool *client.Pool, stmt string, args ...interface{},
) (*mysql.Result, error) {
	return myDo(ctx, pool, func(ctx context.Context, conn *client.Conn) (*mysql.Result, error) {
		return conn.Execute(stmt, args...)
	})
}

func setupMYPool(database ident.Ident) (*client.Pool, func(), error) {
	baseConn, err := client.Connect("127.0.0.1:3306", "root", "SoupOrSecret", "")
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
		"127.0.0.1:3306",                   // address
		"root",                             // user
		"SoupOrSecret",                     // password
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
