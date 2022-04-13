// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package mylogical contains support for reading a mySQL logical
// replication feed.
// It uses Replication with Global Transaction Identifiers.
// See  https://dev.mysql.com/doc/refman/8.0/en/replication-gtids.html
package mylogical

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

// lsnStamp adapts the LSN offset type to a comparable Stamp value.
type lsnStamp int64

var (
	_ stamp.Stamp         = lsnStamp(0)
	_ logical.OffsetStamp = lsnStamp(0)
)

func (s lsnStamp) AsLSN() lsnStamp             { return s }
func (s lsnStamp) AsOffset() uint64            { return uint64(s) }
func (s lsnStamp) Less(other stamp.Stamp) bool { return s < other.(lsnStamp) }

func (s lsnStamp) Marshall() string { return strconv.FormatInt(int64(s), 10) }

func (s lsnStamp) Unmarshall(v string) (stamp.Stamp, error) {
	res, err := strconv.ParseInt(v, 0, 64)
	return lsnStamp(res), err
}

// A Conn encapsulates all wire-connection behavior. It is
// responsible for receiving replication messages and replying with
// status updates.
type Conn struct {
	// Columns, as ordered by the source database.
	columns map[ident.Table][]types.ColData
	// Last GTIDEvent
	lastGTIDEvent int64
	// Map source ids to target tables.
	relations map[uint64]ident.Table
	// The configuration for opening replication connections.
	sourceConfig replication.BinlogSyncerConfig
	// Source Id
	sourceID string
}

// MutationType is the type of mutation
type MutationType int

const (
	insert MutationType = iota
	update
	delete
	unknown
)

func (s MutationType) String() string {
	return [...]string{"insert", "update", "delete", "unknown"}[s]
}

var _ logical.Dialect = (*Conn)(nil)

// NewConn constructs a new pglogical replication feed.
//
// The feed will terminate when the context is canceled and the stopped
// channel will be closed once shutdown is complete.
func NewConn(ctx context.Context, config *Config) (_ *Conn, stopped <-chan struct{}, _ error) {
	if err := config.Preflight(); err != nil {
		return nil, nil, err
	}

	cfg := config.binlogSyncerConfig
	ret := &Conn{
		columns:      make(map[ident.Table][]types.ColData),
		relations:    make(map[uint64]ident.Table),
		sourceConfig: cfg,
		sourceID:     config.CheckPointSourceID,
	}

	var dialect logical.Dialect = ret
	if config.withChaosProb > 0 {
		dialect = logical.WithChaos(dialect, config.withChaosProb)
	}

	stopper, err := logical.Start(ctx, &config.Config, dialect)
	if err != nil {
		return nil, nil, err
	}

	return ret, stopper, nil
}

// Process implements logical.Dialect and receives a sequence of logical
// replication messages, or possibly a rollbackMessage.
func (c *Conn) Process(
	ctx context.Context, ch <-chan logical.Message, events logical.Events,
) error {
	for {
		// Perform context-aware read.
		var msg logical.Message
		select {
		case msg = <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
		// Ensure that we resynchronize.
		if logical.IsRollback(msg) {
			if err := events.OnRollback(ctx, msg); err != nil {
				return err
			}
		}
		var err error
		var ev, ok = msg.(replication.BinlogEvent)
		if ok {
			// See https://dev.mysql.com/doc/internals/en/binlog-event.html
			// Assumptions:
			// We will be handling Row Based Replication Events
			//  https://dev.mysql.com/doc/internals/en/binlog-event.html#:~:text=Row%20Based%20Replication%20Events
			//  Source settings:
			//  binlog_row_image=full  (default setting)
			//  https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_binlog_row_image
			//  binlog_row_metadata = full (default = minimal)
			//  https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_binlog_row_metadata
			//
			log.Tracef("processing %T", ev.Event)
			switch e := ev.Event.(type) {
			case *replication.XIDEvent:
				// On commit should preserve the GTIDs so we can verify consistency,
				// and restart the process from the last committed transaction.
				err = events.OnCommit(ctx)
			case *replication.GTIDEvent:
				// A transaction is executed and committed on the source.
				// This client transaction is assigned a GTID composed of the source's UUID
				// and the smallest nonzero transaction sequence number not yet used on this server (GNO)
				log.Tracef("GTIDEvent %d \n", e.GNO)
				c.lastGTIDEvent = e.GNO

			case *replication.QueryEvent:
				// Only supporting BEGIN
				// DDL statement would also sent here.
				log.Tracef("Query:  %s %+v\n", e.Query, e.GSet)
				if string(e.Query) == "BEGIN" {
					err = events.OnBegin(ctx, lsnStamp(c.lastGTIDEvent))
				}
				//e.Dump(os.Stdout)
			case *replication.TableMapEvent:
				err = c.onRelation(e, events.GetTargetDB())
			case *replication.RowsEvent:
				var operation MutationType
				switch ev.Header.EventType {
				case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
					operation = delete
				case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
					operation = update
				case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
					operation = insert
				default:
					operation = unknown
				}
				mutationCount.With(prometheus.Labels{"type": operation.String()}).Inc()
				if operation != unknown {
					err = c.onDataTuple(ctx, events, e, operation)
				} else {
					err = errors.Errorf("Operation not supported %s", ev.Header.EventType)
				}
			default:
				err = errors.Errorf("unimplemented logical replication message %+v", e)
			}
		} else {
			err = errors.Errorf("unexpected message %T", msg)
		}
		if err != nil {
			return err
		}
	}
}

// ReadInto implements logical.Dialect, opens a replication connection,
// and writes parsed messages into the provided channel. This method
// also manages the keepalive protocol.
func (c *Conn) ReadInto(ctx context.Context, ch chan<- logical.Message, state logical.State) error {

	syncer := replication.NewBinlogSyncer(c.sourceConfig)
	defer syncer.Close()
	// Start sync with specified GTID
	err := state.RestoreConsistentPoint(ctx, lsnStamp(1))
	if err != nil {
		dialFailureCount.Inc()
		return errors.WithStack(err)
	}

	st := fmt.Sprintf("%s:1-%d", c.sourceID, state.GetConsistentPoint())
	gtidset, err := mysql.ParseMysqlGTIDSet(st)
	if err != nil {
		return errors.New("Unable to parse gtidset")
	}
	streamer, err := syncer.StartSyncGTID(gtidset)

	if err != nil {
		dialFailureCount.Inc()
		return errors.WithStack(err)
	}
	dialSuccessCount.Inc()
	standbyTimeout := time.Second * 5
	standbyDeadline := time.Now().Add(standbyTimeout)

	for ctx.Err() == nil {
		if time.Now().After(standbyDeadline) {
			log.Infof("Saving checkpoint %v", state.GetConsistentPoint())
			err = state.SaveConsistentPoint(ctx)
			if err != nil {
				return errors.WithStack(err)
			}
			standbyDeadline = time.Now().Add(standbyTimeout)
		}

		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			return errors.WithStack(err)
		}
		log.Tracef("received %T", ev.Event)
		switch e := ev.Event.(type) {
		case *replication.XIDEvent,
			*replication.GTIDEvent,
			*replication.TableMapEvent,
			*replication.RowsEvent,
			*replication.QueryEvent:
			select {
			case ch <- *ev:
			case <-ctx.Done():
				return errors.WithStack(ctx.Err())
			}
		case *replication.GenericEvent,
			*replication.RotateEvent,
			*replication.PreviousGTIDsEvent:
			// skip these
		case *replication.FormatDescriptionEvent:
			// this is sent when establishing a connection
			// verify that we support the version
			if e.Version != 4 {
				return errors.Errorf("unexpected binlog version %d", e.Version)
			}
			log.Infof("connected to MySQL version %s\n", string(e.ServerVersion))

		default:
			log.Warningf("event type %T is not currently supported", ev.Event)
			ev.Dump(os.Stdout)
		}
	}
	return nil
}

func (c *Conn) onDataTuple(
	ctx context.Context, events logical.Events, tuple *replication.RowsEvent, operation MutationType,
) error {
	tbl, ok := c.relations[tuple.TableID]
	if !ok {
		return errors.Errorf("unknown relation id %d", tuple.TableID)
	}
	if string(tbl.Database().String()) != events.GetTargetDB().String() {
		log.Tracef("Skipping update on %s", tbl.Database().String())
		return nil
	}
	targetCols, ok := c.columns[tbl]
	if !ok {
		return errors.Errorf("no column data for %s", tbl)
	}
	log.Tracef("%s on table %s (#rows: %d)", operation, tbl, len(tuple.Rows))
	for rowNum, row := range tuple.Rows {
		var err error
		var key []interface{}
		var mut types.Mutation
		enc := make(map[string]interface{})
		for idx, sourceCol := range row {
			targetCol := targetCols[idx]
			enc[targetCol.Name.Raw()] = sourceCol
			if targetCol.Primary {
				key = append(key, sourceCol)
			}
		}
		if len(key) == 0 && operation != insert {
			return errors.Errorf("only inserts supported with no key")
		}
		mut.Key, err = json.Marshal(key)
		if err != nil {
			return err
		}
		if operation == update && (rowNum%2 == 0) {
			continue
		}
		if operation != delete {
			mut.Data, err = json.Marshal(enc)
			if err != nil {
				return err
			}
		}
		err = events.OnData(ctx, tbl, []types.Mutation{mut})
		if err != nil {
			return err
		}
	}
	return nil
}

// learn updates the source database namespace mappings.
// Columns names are only available if
// set global binlog_row_metadata = full;
func (c *Conn) onRelation(msg *replication.TableMapEvent, targetDB ident.Ident) error {
	tbl := ident.NewTable(
		ident.New(string(msg.Schema)),
		ident.Public,
		ident.New(string(msg.Table)))
	log.Tracef("Learned %+v", tbl)
	c.relations[msg.TableID] = tbl
	colData := make([]types.ColData, msg.ColumnCount)
	primary := make(map[uint64]bool)
	for _, p := range msg.PrimaryKey {
		primary[p] = true
	}
	if len(msg.ColumnName) != len(msg.ColumnType) {
		return errors.Errorf("all columns names are required 'set global binlog_row_metadata = full'")
	}
	for idx, ctype := range msg.ColumnType {
		_, found := primary[uint64(idx)]
		colData[idx] = types.ColData{
			Ignored: false,
			Name:    ident.New(string(msg.ColumnName[idx])),
			Primary: found,
			Type:    fmt.Sprintf("%d", ctype),
		}
	}
	c.columns[tbl] = colData
	return nil
}
