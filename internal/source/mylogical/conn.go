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
	"strings"

	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

// mySQLStamp adapts mysql.MysqlGTIDSet, so it implements the required String and Less methods.
type mySQLStamp struct {
	gtidset *mysql.MysqlGTIDSet
}

var (
	_ stamp.Stamp = newStamp()
)

func newStamp() mySQLStamp {
	gtidset := new(mysql.MysqlGTIDSet)
	gtidset.Sets = make(map[string]*mysql.UUIDSet)
	return mySQLStamp{
		gtidset: gtidset,
	}
}
func (s mySQLStamp) MarshalText() (text []byte, err error) {
	if s.gtidset == nil {
		return []byte(""), nil
	}
	return []byte(s.gtidset.String()), nil
}
func (s mySQLStamp) Less(other stamp.Stamp) bool {
	if o, ok := other.(mySQLStamp); ok {
		if o.gtidset == nil {
			return false
		}
		if s.gtidset == nil {
			return true
		}
		return o.gtidset.Contain(s.gtidset) && !s.gtidset.Equal(o.gtidset)
	}
	return false
}

func (s mySQLStamp) AddSet() (text []byte, err error) {
	if s.gtidset == nil {
		return []byte(""), nil
	}
	return []byte(s.gtidset.String()), nil
}

// A Conn encapsulates all wire-connection behavior. It is
// responsible for receiving replication messages and replying with
// status updates.
type Conn struct {
	// Columns, as ordered by the source database.
	columns map[ident.Table][]types.ColData
	// Key to set/retrieve state
	consistentPointKey string
	// Last GTIDEvent
	lastGTIDEvent mySQLStamp
	// Map source ids to target tables.
	relations map[uint64]ident.Table
	// The configuration for opening replication connections.
	sourceConfig replication.BinlogSyncerConfig
}

// mutationType is the type of mutation
type mutationType int

const (
	unknown mutationType = iota
	insert
	update
	delete
)

//go:generate go run golang.org/x/tools/cmd/stringer -type=mutationType

var _ logical.Dialect = (*Conn)(nil)

// NewConn constructs a new MySQL replication feed.
//
// The feed will terminate when the context is canceled and the stopped
// channel will be closed once shutdown is complete.
func NewConn(ctx context.Context, config *Config) (_ *Conn, stopped <-chan struct{}, _ error) {
	if err := config.Preflight(); err != nil {
		return nil, nil, err
	}

	cfg := config.binlogSyncerConfig
	ret := &Conn{
		columns:            make(map[ident.Table][]types.ColData),
		consistentPointKey: config.ConsistentPointKey,
		lastGTIDEvent:      newStamp(),
		relations:          make(map[uint64]ident.Table),
		sourceConfig:       cfg,
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
			continue
		}
		var err error
		var ev, ok = msg.(replication.BinlogEvent)
		if !ok {
			return errors.Errorf("unexpected message %T", msg)
		}
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
		// According to https://dev.mysql.com/blog-archive/taking-advantage-of-new-transaction-length-metadata/
		// A DML will start with a GTID event, followed by a QUERY(BEGIN) event,
		// followed by sets of either QUERY events (with their own pre-statement events) or TABLE_MAP and ROWS events,
		// followed by a QUERY(COMMIT|ROLLBACK) or a XID event.
		log.Tracef("processing %T", ev.Event)

	EventProcessing:
		switch e := ev.Event.(type) {
		case *replication.XIDEvent:
			// On commit should preserve the GTIDs so we can verify consistency,
			// and restart the process from the last committed transaction.
			log.Tracef("Commit")
			err = events.OnCommit(ctx)
		case *replication.GTIDEvent:
			// A transaction is executed and committed on the source.
			// This client transaction is assigned a GTID composed of the source's UUID
			// and the smallest nonzero transaction sequence number not yet used on this server (GNO)
			u, _ := uuid.FromBytes(e.SID)
			s := fmt.Sprintf("%s:%d", u.String(), e.GNO)
			a, err := mysql.ParseUUIDSet(s)
			if err == nil {
				if clone, ok := c.lastGTIDEvent.gtidset.Clone().(*mysql.MysqlGTIDSet); ok {
					clone.AddSet(a)
					c.lastGTIDEvent = mySQLStamp{gtidset: clone}
				}
			}

		case *replication.QueryEvent:
			// Only supporting BEGIN
			// DDL statement would also sent here.
			log.Tracef("Query:  %s %+v\n", e.Query, e.GSet)
			if string(e.Query) == "BEGIN" {
				err = events.OnBegin(ctx, c.lastGTIDEvent)
			}
		case *replication.TableMapEvent:
			err = c.onRelation(e, events.GetTargetDB())
		case *replication.RowsEvent:
			var operation mutationType
			switch ev.Header.EventType {
			case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				operation = delete
			case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				operation = update
			case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				operation = insert
			default:
				err = errors.Errorf("Operation not supported %s", ev.Header.EventType)
				break EventProcessing
			}
			mutationCount.With(prometheus.Labels{"type": operation.String()}).Inc()
			err = c.onDataTuple(ctx, events, e, operation)
		default:
			err = errors.Errorf("unimplemented logical replication message %+v", e)
		}
		if err != nil {
			return err
		}
	}
}

// ReadInto implements logical.Dialect, opens a replication connection,
// and writes supported events into the provided channel.
func (c *Conn) ReadInto(ctx context.Context, ch chan<- logical.Message, state logical.State) error {
	syncer := replication.NewBinlogSyncer(c.sourceConfig)
	defer syncer.Close()
	if state.GetConsistentPoint() == nil {
		return errors.New("missing gtidset")
	}
	log.Tracef("ReadInto: %+v", state)
	m, err := state.GetConsistentPoint().MarshalText()
	if err != nil {
		return errors.Wrap(err, "unable to parse gtidset")
	}
	gtidset, err := mysql.ParseGTIDSet(mysql.MySQLFlavor, string(m))
	if err != nil {
		return errors.Wrap(err, "unable to parse gtidset")
	}
	streamer, err := syncer.StartSyncGTID(gtidset)

	if err != nil {
		dialFailureCount.Inc()
		return errors.WithStack(err)
	}
	dialSuccessCount.Inc()
	if a, ok := gtidset.(*mysql.MysqlGTIDSet); ok {
		c.lastGTIDEvent.gtidset = a
	}

	for ctx.Err() == nil {
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
				return errors.Wrap(ctx.Err(), "error while receiving events")
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
			log.Infof("connected to MySQL version %s", strings.Trim(string(e.ServerVersion), "\x00"))

		default:
			log.Warningf("event type %T is not currently supported", ev.Event)
			if log.IsLevelEnabled(log.TraceLevel) {
				ev.Dump(os.Stdout)
			}
		}
	}
	return nil
}

// UnmarshalStamp decodes GTID Sets expressed as strings.
// See https://dev.mysql.com/doc/refman/8.0/en/replication-gtids-concepts.html
// Examples:
// E11FA47-71CA-11E1-9E33-C80AA9429562:1-3:11:47-49
// 2174B383-5441-11E8-B90A-C80AA9429562:1-3, 24DA167-0C0C-11E8-8442-00059A3C7B00:1-19
func (c *Conn) UnmarshalStamp(stamp []byte) (stamp.Stamp, error) {
	log.Tracef("UnmarshalStamp %s", stamp)
	s, err := mysql.ParseGTIDSet(mysql.MySQLFlavor, string(stamp))
	if err != nil {
		return nil, errors.Wrapf(err, "cannot unmarshal stamp %s", string(stamp))
	}
	ret, ok := s.(*mysql.MysqlGTIDSet)
	if !ok {
		return nil, errors.New("cannot unmarshal stamp " + string(stamp))
	}
	return mySQLStamp{gtidset: ret}, nil
}

func (c *Conn) onDataTuple(
	ctx context.Context, events logical.Events, tuple *replication.RowsEvent, operation mutationType,
) error {
	tbl, ok := c.relations[tuple.TableID]
	if !ok {
		return errors.Errorf("unknown relation id %d", tuple.TableID)
	}
	if tbl.Database() != events.GetTargetDB() {
		log.Tracef("Skipping update on %s", tbl.Database())
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
			switch targetCol.Type {
			case mysql.MYSQL_TYPE_JSON:
				if sourceCol == nil {
					enc[targetCol.Name.Raw()] = nil
				} else if s, ok := sourceCol.([]byte); ok {
					enc[targetCol.Name.Raw()] = string(s)
				} else {
					log.Warnf("Cannot convert %T to string", sourceCol)
				}
			case mysql.MYSQL_TYPE_BIT:
				if sourceCol == nil {
					enc[targetCol.Name.Raw()] = nil
				} else if s, ok := sourceCol.(int64); ok {
					enc[targetCol.Name.Raw()] = strconv.FormatInt(s, 2)
				} else {
					log.Warnf("Cannot convert %t to binary", sourceCol)
				}
			default:
				enc[targetCol.Name.Raw()] = sourceCol
			}

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
		// on update we only care about the new value.
		// even rows are skipped since they contain the value before the update
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

// onRelation updates the source database namespace mappings.
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
		return errors.New("all columns names are required 'set global binlog_row_metadata = full'")
	}
	for idx, ctype := range msg.ColumnType {
		_, found := primary[uint64(idx)]
		colData[idx] = types.ColData{
			Ignored: false,
			Name:    ident.New(string(msg.ColumnName[idx])),
			Primary: found,
			Type:    ctype,
		}
	}
	c.columns[tbl] = colData
	return nil
}
