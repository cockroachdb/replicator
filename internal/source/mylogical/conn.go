// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package mylogical contains code for reading a logical replication
// feed from a MySQL (or compatible) database.
package mylogical

import (
	"context"
	"reflect"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/go-mysql-org/go-mysql/mysql"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/cockroachdb/cdc-sink/internal/source/cdc"
	"github.com/cockroachdb/cdc-sink/internal/target/apply"
	"github.com/cockroachdb/cdc-sink/internal/target/schemawatch"
	"github.com/cockroachdb/cdc-sink/internal/target/stage"
	"github.com/cockroachdb/cdc-sink/internal/target/timekeeper"
	"github.com/jackc/pgx/v4"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v4/pgxpool"
)

// Conn encapsulates a logical replication feed to a source database.
type Conn struct {
	// Apply mutations to the backing store.
	appliers types.Appliers
	// The active configuration.
	cfg *Config
	// Columns, as ordered by the source database.
	columns map[ident.Table][]types.ColData
	// Tables that need to be drained.
	dirty     map[ident.Table]struct{}
	immediate bool
	// Batches of mutations to stage.
	pending map[ident.Table][]types.Mutation
	// Callbacks to release the slices in pending.
	pendingReleases []func()
	// The pg publication name to subscribe to.
	publicationName string
	// Map source ids to target tables.
	relations map[uint32]ident.Table
	// The amount of time to sleep between retries of the replication
	// loop.
	retryDelay time.Duration
	// Stage mutations in the backing store.
	stagers types.Stagers
	// The name of the slot within the publication.
	slotName string
	// The SQL database we're going to be writing into.
	targetDB ident.Ident
	// Connection string for the target database.
	targetPool *pgxpool.Pool
	// Likely nil.
	testControls *TestControls
	// Drain.
	timeKeeper types.TimeKeeper
	// The (eventual) commit time of the transaction being processed.
	txTime hlc.Time

	mu struct {
		sync.Mutex

		// This is the position in the transaction log that we'll
		// occasionally report back to the server. It is updated when we
		// successfully commit an entire transaction's worth of data.
		clientXLogPos pglogrepl.LSN
	}
}

func NewConn(ctx context.Context, config *Config) (_ *Conn, stopped <-chan struct{}, _ error) {
	if err := config.preflight(); err != nil {
		return nil, nil, err
	}
	// Bring up connection to target database.
	targetCfg, err := pgxpool.ParseConfig(config.TargetConn)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "could not parse %q", config.TargetConn)
	}
	// Identify traffic.
	targetCfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "SET application_name=$1", "cdc-sink")
		return err
	}
	// Ensure connection diversity through long-lived loadbalancers.
	targetCfg.MaxConnLifetime = 10 * time.Minute
	// Keep one spare connection.
	targetCfg.MinConns = 1

	targetPool, err := pgxpool.ConnectConfig(ctx, targetCfg)
	if err != nil {
		return nil, nil, errors.Wrap(err, "could not connect to CockroachDB")
	}

	timeKeeper, err := timekeeper.NewTimeKeeper(ctx, targetPool, cdc.Resolved)
	if err != nil {
		return nil, nil, err
	}

	watchers, cancelWatchers := schemawatch.NewWatchers(targetPool)
	appliers, cancelAppliers := apply.NewAppliers(watchers)
	stagers := stage.NewStagers(targetPool, ident.StagingDB)

	ret := &Conn{
		appliers:     appliers,
		cfg:          config.Copy(),
		columns:      make(map[ident.Table][]types.ColData),
		dirty:        make(map[ident.Table]struct{}),
		immediate:    config.Immediate,
		pending:      make(map[ident.Table][]types.Mutation),
		relations:    make(map[uint32]ident.Table),
		retryDelay:   config.RetryDelay,
		stagers:      stagers,
		targetDB:     config.TargetDB,
		targetPool:   targetPool,
		testControls: config.TestControls,
		timeKeeper:   timeKeeper,
	}

	stopper := make(chan struct{})
	go func() {
		_ = ret.run(ctx)
		cancelAppliers()
		cancelWatchers()
		targetPool.Close()
		close(stopper)
	}()

	return ret, stopper, nil
}

func (c *Conn) run(ctx context.Context) error {
	panic(nil)
}

// readReplicationData opens a replication connection and writes parsed
// messages into the provided channel.
//
// The underlying client is generally robust against connection
// difficulties, but we still implement an outer retry loop.
// TODO(bob) UPDATE THIS:
//   The consuming code should call Conn.setLogPos
//   in order to advance the server-side high-water mark.
func (c *Conn) readReplicationData(ctx context.Context, ch chan<- *replication.BinlogEvent) error {
	cfg := replication.BinlogSyncerConfig{
		ServerID:                uint32(c.cfg.SourceServerID),
		Flavor:                  "mysql", // TODO(bob): mariadb option
		Host:                    c.cfg.SourceHost,
		Port:                    uint16(c.cfg.SourcePort),
		User:                    c.cfg.SourceUser,
		Password:                c.cfg.SourcePassword,
		Charset:                 "utf8",
		ParseTime:               true, // Reify into time.Time values.
		TimestampStringLocation: time.UTC,
	}

	syncer := replication.NewBinlogSyncer(cfg)

	streamer, err := syncer.StartSyncGTID(&mysql.MysqlGTIDSet{})
	if err != nil {
		return errors.WithStack(err)
	}

	for {
		evt, err := streamer.GetEvent(ctx)
		if err != nil {
			return errors.WithStack(err)
		}

		w := log.WithFields(log.Fields{"event": reflect.TypeOf(evt).String()}).Writer()

		switch t := evt.Event.(type) {
		case *replication.GTIDEvent:
			t.Dump(w)
		case *replication.TableMapEvent:
			t.Dump(w)
		case *replication.RowsEvent:
			t.Dump(w)
		default:

		}
		w.Close()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case ch <- evt:
			// OK.
		}
	}
}
