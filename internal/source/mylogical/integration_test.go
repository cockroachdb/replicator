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
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/target/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

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
	if err := myExec(ctx, myPool,
		fmt.Sprintf(`CREATE TABLE %s (k INT PRIMARY KEY, v TEXT)`, tgt.Table().Raw()),
	); !a.NoError(err) {
		return
	}
	if _, err := crdbPool.Exec(ctx,
		fmt.Sprintf(`CREATE TABLE %s (k INT PRIMARY KEY, v TEXT)`, tgt)); !a.NoError(err) {
		return
	}

	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID:                666,
		Flavor:                  "mysql",
		Host:                    "127.0.0.1",
		Port:                    3306,
		User:                    "root",
		Password:                "SoupOrSecret",
		Localhost:               "",
		Charset:                 "utf8",
		SemiSyncEnabled:         false,
		RawModeEnabled:          false,
		TLSConfig:               nil,
		ParseTime:               true,
		TimestampStringLocation: time.UTC,
		UseDecimal:              false,
		RecvBufferSize:          0,
		HeartbeatPeriod:         0,
		ReadTimeout:             0,
		MaxReconnectAttempts:    0,
		DisableRetrySync:        false,
		VerifyChecksum:          false,
		DumpCommandFlag:         0,
		Option:                  nil,
	})

	streamer, err := syncer.StartSyncGTID(&mysql.MysqlGTIDSet{})
	if !a.NoError(err) {
		return
	}

	// Insert data into source table.
	const rowCount = 1024
	if err := myDo(ctx, myPool,
		func(ctx context.Context, conn *client.Conn) error {
			if err := conn.Begin(); err != nil {
				return err
			}
			defer conn.Rollback()

			for i := 0; i < rowCount; i++ {
				if _, err := conn.Execute(
					fmt.Sprintf("INSERT INTO %s VALUES (?, ?)", tgt.Table().Raw()),
					i, fmt.Sprintf("v=%d", i),
				); err != nil {
					return err
				}
			}

			return conn.Commit()
		},
	); !a.NoError(err) {
		return
	}

	f, err := os.Create("/tmp/test.out")
	if !a.NoError(err) {
		return
	}
	defer f.Close()

	for {
		evtCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		ev, err := streamer.GetEvent(evtCtx)
		cancel()
		if !a.NoError(err) {
			return
		}

		ev.Event.Dump(f)
	}
}

func myDo(
	ctx context.Context, pool *client.Pool, fn func(context.Context, *client.Conn) error,
) error {
	conn, err := pool.GetConn(ctx)
	if err != nil {
		return err
	}
	defer pool.PutConn(conn)

	return fn(ctx, conn)
}

// myExec is similar in spirit to pgx.Exec.
func myExec(ctx context.Context, pool *client.Pool, stmt string, args ...interface{}) error {
	return myDo(ctx, pool, func(ctx context.Context, conn *client.Conn) error {
		_, err := conn.Execute(stmt, args...)
		return err
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
