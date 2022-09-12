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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

// conn encapsulates all wire-connection behavior. It is
// responsible for receiving replication messages and replying with
// status updates.
type conn struct {
	// Columns, as ordered by the source database.
	columns map[ident.Table][]types.ColData
	// Flavor is one of the mysql.MySQLFlavor or mysql.MariaDBFlavor constants
	flavor string
	// Map source ids to target tables.
	relations map[uint64]ident.Table
	// The configuration for opening replication connections.
	sourceConfig replication.BinlogSyncerConfig
}

// mutationType is the type of mutation
type mutationType int

const (
	unknownMutation mutationType = iota
	insertMutation
	updateMutation
	deleteMutation
)

//go:generate go run golang.org/x/tools/cmd/stringer -type=mutationType

var _ logical.Dialect = (*conn)(nil)

// Process implements logical.Dialect and receives a sequence of logical
// replication messages, or possibly a rollbackMessage.
func (c *conn) Process(
	ctx context.Context, ch <-chan logical.Message, events logical.Events,
) error {
	// This is the expected consistent point (i.e. transaction id) that
	// we expect to see, given all previous messages on the wire. It is
	// set, and reset, any time the upstream producer (re-)starts a read
	// from the transaction log.
	var streamCP *consistentPoint

	for msg := range ch {
		// Ensure that we resynchronize.
		if logical.IsRollback(msg) {
			if err := events.OnRollback(ctx, msg); err != nil {
				return err
			}
			continue
		}
		// Resynchronize with the view of consumed transactions.
		if nextStamp, ok := msg.(*consistentPoint); ok {
			streamCP = nextStamp
			continue
		}
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
		// MySQL:
		// According to https://dev.mysql.com/blog-archive/taking-advantage-of-new-transaction-length-metadata/
		// A DML will start with a GTID event, followed by a QUERY(BEGIN) event,
		// followed by sets of either QUERY events (with their own pre-statement events) or TABLE_MAP and ROWS events,
		// followed by a QUERY(COMMIT|ROLLBACK) or a XID event.
		//
		// MariaDB:
		// we expect a MariadbGTIDEvent with the GTID to begin the transaction
		log.Tracef("processing %T", ev.Event)

		switch e := ev.Event.(type) {
		case *replication.XIDEvent:
			// On commit should preserve the GTIDs so we can verify consistency,
			// and restart the process from the last committed transaction.
			log.Tracef("Commit")
			if err := events.OnCommit(ctx); err != nil {
				return err
			}

		case *replication.GTIDEvent:
			// A transaction is executed and committed on the source.
			// This client transaction is assigned a GTID composed of the source's UUID
			// and the smallest nonzero transaction sequence number not yet used on this server (GNO)
			u, err := uuid.FromBytes(e.SID)
			if err != nil {
				return err
			}
			ns := fmt.Sprintf("%s:%d", u.String(), e.GNO)
			toAdd, err := mysql.ParseUUIDSet(ns)
			if err != nil {
				return err
			}
			streamCP = streamCP.withMysqlGTIDSet(e.OriginalCommitTime(), toAdd)

		case *replication.MariadbGTIDEvent:
			// We ignore events that won't have a terminating COMMIT
			// events, e.g. schema changes.
			// See flags section: https://mariadb.com/kb/en/gtid_event/
			if e.IsStandalone() {
				continue
			}
			ts := time.Unix(int64(ev.Header.Timestamp), 0)
			var err error
			streamCP, err = streamCP.withMariaGTIDSet(ts, &e.GTID)
			if err != nil {
				return err
			}
			if err := events.OnBegin(ctx, streamCP); err != nil {
				return err
			}

		case *replication.QueryEvent:
			// Only supporting BEGIN
			// DDL statement would also sent here.
			log.Tracef("Query:  %s %+v\n", e.Query, e.GSet)
			if bytes.Equal(e.Query, []byte("BEGIN")) {
				if err := events.OnBegin(ctx, streamCP); err != nil {
					return err
				}
			}

		case *replication.TableMapEvent:
			if err := c.onRelation(e); err != nil {
				return err
			}

		case *replication.RowsEvent:
			var operation mutationType
			switch ev.Header.EventType {
			case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				operation = deleteMutation
			case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				operation = updateMutation
			case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				operation = insertMutation
			default:
				return errors.Errorf("Operation not supported %s", ev.Header.EventType)
			}
			mutationCount.With(prometheus.Labels{"type": operation.String()}).Inc()
			if err := c.onDataTuple(ctx, events, e, operation); err != nil {
				return err
			}

		default:
			return errors.Errorf("unimplemented logical replication message %+v", e)
		}
	}
	return nil
}

// ReadInto implements logical.Dialect, opens a replication connection,
// and writes supported events into the provided channel.
func (c *conn) ReadInto(ctx context.Context, ch chan<- logical.Message, state logical.State) error {
	syncer := replication.NewBinlogSyncer(c.sourceConfig)
	defer syncer.Close()

	cp := state.GetConsistentPoint()
	if cp == nil {
		return errors.New("missing gtidset")
	}
	streamer, err := syncer.StartSyncGTID(cp.(*consistentPoint).AsGTIDSet())
	if err != nil {
		dialFailureCount.Inc()
		return err
	}
	dialSuccessCount.Inc()

	// Send the initial consistent point we're reading from.
	select {
	case ch <- cp.(*consistentPoint):
	case <-ctx.Done():
		return ctx.Err()
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
			*replication.QueryEvent,
			*replication.MariadbGTIDEvent,
			*replication.MariadbAnnotateRowsEvent:
			select {
			case ch <- *ev:
			case <-ctx.Done():
				return errors.Wrap(ctx.Err(), "error while receiving events")
			}
		case *replication.GenericEvent,
			*replication.RotateEvent,
			*replication.PreviousGTIDsEvent,
			*replication.MariadbGTIDListEvent,
			*replication.MariadbBinlogCheckPointEvent:
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

// ZeroStamp implements logical.Dialect.
func (c *conn) ZeroStamp() stamp.Stamp {
	return newConsistentPoint(c.flavor)
}

func (c *conn) onDataTuple(
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
		var key []any
		var mut types.Mutation
		enc := make(map[string]any)
		// on update we only care about the new value.
		// even rows are skipped since they contain the value before the update
		if operation == updateMutation && (rowNum%2 == 0) {
			continue
		}
		for idx, sourceCol := range row {
			targetCol := targetCols[idx]
			switch s := sourceCol.(type) {
			case nil:
				enc[targetCol.Name.Raw()] = nil
			case []byte:
				enc[targetCol.Name.Raw()] = string(s)
			case int64:
				// if it's a bit need to convert to a string representation
				if targetCol.Type == mysql.MYSQL_TYPE_BIT {
					enc[targetCol.Name.Raw()] = strconv.FormatInt(s, 2)
				} else {
					enc[targetCol.Name.Raw()] = s
				}
			default:
				enc[targetCol.Name.Raw()] = s
			}
			if targetCol.Primary {
				key = append(key, sourceCol)
			}
		}
		if len(key) == 0 && operation != insertMutation {
			return errors.Errorf("only inserts supported with no key")
		}
		mut.Key, err = json.Marshal(key)
		if err != nil {
			return err
		}
		if operation != deleteMutation {
			mut.Data, err = json.Marshal(enc)
			if err != nil {
				return err
			}
		}
		err = events.OnData(ctx, tbl.Table(), tbl, []types.Mutation{mut})
		if err != nil {
			return err
		}
	}
	return nil
}

// onRelation updates the source database namespace mappings.
// Columns names are only available if
// set global binlog_row_metadata = full;
func (c *conn) onRelation(msg *replication.TableMapEvent) error {
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

var (
	// Required settings. { {"system variable", "expected value"}}
	mySQLSystemSettings = [][]string{
		{"gtid_mode", "ON"},
		{"enforce_gtid_consistency", "ON"},
		{"binlog_row_metadata", "FULL"},
	}
	mariaDBSystemSettings = [][]string{
		{"log_bin", "1"},
		{"binlog_format", "ROW"},
		{"binlog_row_metadata", "FULL"},
	}
)

// getFlavor connects to the server and tries to determine the type of server by looking at the
// @@version_comment system variable.
// Based on the type of server it also verifies that the settings defined in the
// mySQLSystemSettings and mariaDBSystemSettings slices are correctly configured for the replication to work.
// It returns mysql.MariaDBFlavor or mysql.MySQLFlavor upon success.
func getFlavor(config *Config) (string, error) {
	addr := fmt.Sprintf("%s:%d", config.host, config.port)
	c, err := client.Connect(addr, config.user, config.password, "", func(c *client.Conn) {
		c.SetTLSConfig(config.tlsConfig)
	})
	if err != nil {
		return "", err
	}
	defer c.Close()
	res, err := c.Execute("select @@version_comment;")
	if err != nil {
		return "", err
	}
	if len(res.Values) == 0 {
		return "", errors.New("unable to retrieve version")
	}

	version := string(res.Values[0][0].AsString())
	log.Infof("Version info: %s", version)
	if strings.Contains(version, "mariadb") {
		for _, v := range mariaDBSystemSettings {
			err = checkSystemSetting(c, v[0], v[1])
			if err != nil {
				return "", err
			}
		}
		return mysql.MariaDBFlavor, nil
	} else if strings.Contains(version, "MySQL") {
		for _, v := range mySQLSystemSettings {
			err = checkSystemSetting(c, v[0], v[1])
			if err != nil {
				return "", err
			}
		}
		return mysql.MySQLFlavor, nil
	} else {
		return "", errors.New("unknown server")
	}
}

func checkSystemSetting(c *client.Conn, variable string, expected string) error {
	res, err := c.Execute(fmt.Sprintf("select @@%s;", variable))
	if err != nil {
		return err
	}
	if len(res.Values) == 0 {
		return errors.New("unable to retrieve system setting")
	}

	var value string
	switch res.Values[0][0].Type {
	case mysql.FieldValueTypeSigned:
		value = strconv.FormatInt(res.Values[0][0].AsInt64(), 10)
	case mysql.FieldValueTypeUnsigned:
		value = strconv.FormatUint(res.Values[0][0].AsUint64(), 10)
	default:
		value = string(res.Values[0][0].AsString())
	}
	if value != expected {
		return errors.Errorf("invalid server setting for %s. Expected %s, found %s", variable, expected, value)
	}
	return nil
}
