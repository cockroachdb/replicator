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

// Package mylogical contains support for reading a mySQL logical
// replication feed.
// It uses Replication with Global Transaction Identifiers.
// See  https://dev.mysql.com/doc/refman/8.0/en/replication-gtids.html
package mylogical

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/field-eng-powertools/stopvar"
	"github.com/cockroachdb/replicator/internal/script"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/diag"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/stamp"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

// Conn exports the package-internal type.
type Conn conn

// conn encapsulates all wire-connection behavior. It is
// responsible for receiving replication messages and replying with
// status updates.
type conn struct {
	// The destination for writes.
	acceptor types.MultiAcceptor
	// Columns, as ordered by the source database.
	columns *ident.TableMap[[]types.ColData]
	// The connector configuration.
	config *Config
	// The consistent point of the transaction being accumulated.
	nextConsistentPoint *consistentPoint
	// Persistent storage for WAL data.
	memo types.Memo
	// Flavor is one of the mysql.MySQLFlavor or mysql.MariaDBFlavor constants
	flavor string
	// Map source ids to target tables.
	relations map[uint64]ident.Table
	// The configuration for opening replication connections.
	sourceConfig replication.BinlogSyncerConfig
	// Access to the staging cluster.
	stagingDB *types.StagingPool
	// The destination for writes.
	target ident.Schema
	// Access to the target database.
	targetDB *types.TargetPool
	// Managed by persistWALOffset.
	walOffset notify.Var[*consistentPoint]
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

var _ diag.Diagnostic = (*conn)(nil)

// Diagnostic implements [diag.Diagnostic].
func (c *conn) Diagnostic(_ context.Context) any {
	return map[string]any{
		"columns":   c.columns,
		"flavor":    c.flavor,
		"relations": c.relations,
	}
}

func (c *conn) Start(ctx *stopper.Context) error {
	// Initialize this field once.
	c.nextConsistentPoint = newConsistentPoint(c.flavor)

	// Call this first to load the previous offset. We want to reset our
	// state before starting the main copier routine.
	if err := c.persistWALOffset(ctx); err != nil {
		return err
	}

	// Start a process to copy data to the target.
	ctx.Go(func(ctx *stopper.Context) error {
		for !ctx.IsStopping() {
			if err := c.copyMessages(ctx); err != nil {
				log.WithError(err).Warn("error while copying messages; will retry")
				select {
				case <-ctx.Stopping():
				case <-time.After(time.Second):
				}
			}
		}
		return nil
	})

	return nil
}

// Process implements logical.Dialect and receives a sequence of logical
// replication messages, or possibly a rollbackMessage.
func (c *conn) accumulateBatch(
	ctx *stopper.Context, ev *replication.BinlogEvent, batch *types.MultiBatch,
) (*types.MultiBatch, error) {
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
		if batch.Count() == 0 {
			log.Trace("skipping empty transaction")
		} else {
			tx, err := c.targetDB.BeginTx(ctx, &sql.TxOptions{})
			if err != nil {
				return nil, errors.WithStack(err)
			}
			defer tx.Rollback()

			if err := c.acceptor.AcceptMultiBatch(ctx, batch, &types.AcceptOptions{
				TargetQuerier: tx,
			}); err != nil {
				return nil, err
			}
			if err := tx.Commit(); err != nil {
				return nil, errors.WithStack(err)
			}
		}

		c.walOffset.Set(c.nextConsistentPoint)
		return nil, nil

	case *replication.GTIDEvent:
		// A transaction is executed and committed on the source.
		// This client transaction is assigned a GTID composed of the source's UUID
		// and the smallest nonzero transaction sequence number not yet used on this server (GNO)
		u, err := uuid.FromBytes(e.SID)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		ns := fmt.Sprintf("%s:%d", u.String(), e.GNO)
		toAdd, err := mysql.ParseUUIDSet(ns)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		c.nextConsistentPoint = c.nextConsistentPoint.withMysqlGTIDSet(e.OriginalCommitTime(), toAdd)
		// Expect to see a QueryEvent to create the batch.
		return nil, nil

	case *replication.MariadbGTIDEvent:
		// We ignore events that won't have a terminating COMMIT
		// events, e.g. schema changes.
		// See flags section: https://mariadb.com/kb/en/gtid_event/
		if e.IsStandalone() {
			return nil, nil
		}
		ts := time.Unix(int64(ev.Header.Timestamp), 0)
		var err error
		c.nextConsistentPoint, err = c.nextConsistentPoint.withMariaGTIDSet(ts, &e.GTID)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		// Return a batch to accumulate data, since this event
		// represents a transaction.
		return &types.MultiBatch{}, nil

	case *replication.QueryEvent:
		// Only supporting BEGIN
		// DDL statement would also sent here.
		log.Tracef("Query:  %s %+v\n", e.Query, e.GSet)
		if bytes.Equal(e.Query, []byte("BEGIN")) {
			return &types.MultiBatch{}, nil
		}

	case *replication.TableMapEvent:
		if err := c.onRelation(e); err != nil {
			return batch, err
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
			return nil, errors.Errorf("Operation not supported %s", ev.Header.EventType)
		}
		mutationCount.With(prometheus.Labels{"type": operation.String()}).Inc()
		if err := c.onDataTuple(batch, e, operation); err != nil {
			return nil, err
		}

	default:
		return nil, errors.Errorf("unimplemented logical replication message %+v", e)
	}
	return batch, nil
}

// copyMessages is the main replication loop. It will open a connection
// to the source, accumulate messages, and commit data to the target.
func (c *conn) copyMessages(ctx *stopper.Context) error {
	syncer := replication.NewBinlogSyncer(c.sourceConfig)
	defer syncer.Close()

	cp, _ := c.walOffset.Get()
	if cp == nil {
		return errors.New("missing gtidset")
	}
	streamer, err := syncer.StartSyncGTID(cp.AsGTIDSet())
	if err != nil {
		dialFailureCount.Inc()
		return err
	}
	dialSuccessCount.Inc()

	var batch *types.MultiBatch

	for {
		// Make GetEvent interruptable.
		eventCtx, cancelEventRead := context.WithCancel(ctx)
		go func() {
			select {
			case <-eventCtx.Done():
			case <-ctx.Stopping():
			}
			cancelEventRead()
		}()

		ev, err := streamer.GetEvent(eventCtx)
		cancelEventRead()
		if errors.Is(err, context.Canceled) {
			return nil
		} else if err != nil {
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
			batch, err = c.accumulateBatch(ctx, ev, batch)
			if err != nil {
				return err
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
}

// ZeroStamp implements logical.Dialect.
func (c *conn) ZeroStamp() stamp.Stamp {
	return newConsistentPoint(c.flavor)
}

func (c *conn) onDataTuple(
	batch types.Accumulator, tuple *replication.RowsEvent, operation mutationType,
) error {
	tbl, ok := c.relations[tuple.TableID]
	if !ok {
		return errors.Errorf("unknown relation id %d", tuple.TableID)
	}
	if !c.target.Contains(tbl) {
		log.Tracef("Skipping update on %s because it is not in the target schema %s", tbl, c.target)
		return nil
	}
	targetCols, ok := c.columns.Get(tbl)
	if !ok {
		return errors.Errorf("no column data for %s", tbl)
	}
	log.Tracef("%s on table %s (#rows: %d)", operation, tbl, len(tuple.Rows))
	for rowNum, row := range tuple.Rows {
		// on update we only care about the new value.
		// even rows are skipped since they contain the value before the update
		if operation == updateMutation && (rowNum%2 == 0) {
			continue
		}
		if len(targetCols) != len(row) {
			return errors.Errorf("unexpected number of columns in the logical stream for %s", tbl)
		}
		var err error
		var key []any
		var mut types.Mutation
		enc := make(map[string]any)
		for idx, sourceCol := range row {
			targetCol := targetCols[idx]
			switch s := sourceCol.(type) {
			case nil:
				enc[targetCol.Name.Raw()] = nil
			case []byte:
				enc[targetCol.Name.Raw()] = string(s)
			case int64:
				// if it's a bit need to convert to a string representation
				if targetCol.Type == fmt.Sprintf("%d", mysql.MYSQL_TYPE_BIT) {
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
			return errors.Errorf("only inserts supported with no key for %s", tbl)
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
		mut.Time = hlc.New(c.nextConsistentPoint.AsTime().UnixNano(), 0)
		script.AddMeta("mylogical", tbl, &mut)
		if err := batch.Accumulate(tbl, mut); err != nil {
			return err
		}
	}
	return nil
}

// getTableMetadata fetches table metadata from the database
// if binlog_row_metadata = minimal
func (c *conn) getColNames(table ident.Table) ([][]byte, []uint64, error) {
	cl, err := getConnection(c.config)
	if err != nil {
		return nil, nil, err
	}
	defer cl.Close()
	db, _ := table.Schema().Split()
	log.Tracef("getting col names %s %s", db, table.Table().String())
	res, err := cl.Execute(`
		SELECT COLUMN_NAME, COLUMN_KEY='PRI'
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION;
	`, db.Raw(), table.Table().Raw())

	if err != nil {
		return nil, nil, err
	}
	names := make([][]byte, 0, len(res.Values))
	keys := make([]uint64, 0, len(res.Values))
	for i, row := range res.Values {
		names = append(names, row[0].AsString())
		if row[1].AsInt64() == 1 {
			keys = append(keys, uint64(i))
		}
	}
	return names, keys, nil
}

// onRelation updates the source database namespace mappings.
// Columns names are only available if
// set global binlog_row_metadata = full;
func (c *conn) onRelation(msg *replication.TableMapEvent) error {
	tbl := ident.NewTable(
		ident.MustSchema(ident.New(string(msg.Schema)), ident.Public),
		ident.New(string(msg.Table)))
	log.Tracef("Learned %+v", tbl)
	columnNames, primaryKeys := msg.ColumnName, msg.PrimaryKey
	// In case we need to fetch the metadata directly from the
	// source, we will do it the first time we see the table,
	// to avoid calls for each row event (for older version of MySQL)
	if c.config.FetchMetadata {
		if _, ok := c.relations[msg.TableID]; ok {
			return nil
		}
		var err error
		columnNames, primaryKeys, err = c.getColNames(tbl)
		if err != nil {
			return err
		}
	}
	c.relations[msg.TableID] = tbl
	colData := make([]types.ColData, msg.ColumnCount)
	primary := make(map[uint64]bool)
	for _, p := range primaryKeys {
		primary[p] = true
	}
	if len(columnNames) != len(msg.ColumnType) {
		return errors.Errorf("all columns names are required. Got %d columns and %d names.",
			len(msg.ColumnType), len(columnNames))
	}
	for idx, ctype := range msg.ColumnType {
		_, found := primary[uint64(idx)]
		colData[idx] = types.ColData{
			Ignored: false,
			Name:    ident.New(string(columnNames[idx])),
			Primary: found,
			Type:    fmt.Sprintf("%d", ctype),
		}
	}
	c.columns.Put(tbl, colData)
	return nil
}

var (
	// Required settings. { {"system variable", "expected values" ...}}
	mySQLSystemSettings = [][]string{
		{"gtid_mode", "ON", "1"},
		{"enforce_gtid_consistency", "ON", "1"},
		{"binlog_row_metadata", "FULL"},
	}

	mySQL5SystemSettings = [][]string{
		{"gtid_mode", "ON", "1"},
		{"enforce_gtid_consistency", "ON", "1"},
		{"binlog_row_image", "FULL"},
		{"binlog_format", "ROW"},
		{"log_bin", "1"},
	}
	mariaDBSystemSettings = [][]string{
		{"log_bin", "1"},
		{"binlog_format", "ROW"},
		{"binlog_row_metadata", "FULL"},
	}
)

// getConnection returns a connection to the source database
func getConnection(config *Config) (*client.Conn, error) {
	addr := fmt.Sprintf("%s:%d", config.host, config.port)
	return client.Connect(addr, config.user, config.password, "", func(c *client.Conn) {
		c.SetTLSConfig(config.tlsConfig)
	})
}

// getFlavor connects to the server and tries to determine the type of server by looking at the
// @@version_comment system variable.
// Based on the type of server it also verifies that the settings defined in the
// mySQLSystemSettings and mariaDBSystemSettings slices are correctly configured for the replication to work.
// It returns mysql.MariaDBFlavor or mysql.MySQLFlavor upon success, together with the version information
func getFlavor(config *Config) (string, string, error) {
	c, err := getConnection(config)
	if err != nil {
		return "", "", err
	}
	defer c.Close()
	res, err := c.Execute("select @@version;")
	if err != nil {
		return "", "", err
	}
	if len(res.Values) == 0 {
		return "", "", errors.New("unable to retrieve version")
	}

	version := string(res.Values[0][0].AsString())
	log.Infof("Version info: %s", version)
	if strings.Contains(strings.ToLower(version), "mariadb") {
		for _, v := range mariaDBSystemSettings {
			err = checkSystemSetting(c, v[0], v[1:])
			if err != nil {
				return "", "", err
			}
		}
		return mysql.MariaDBFlavor, version, nil
	}
	if strings.HasPrefix(version, "5.") {
		log.Warn("Detecting MySQL 5.X; forcing metadata fetch")
		config.FetchMetadata = true
	}
	if config.FetchMetadata {
		for _, v := range mySQL5SystemSettings {
			err = checkSystemSetting(c, v[0], v[1:])
			if err != nil {
				return "", "", err
			}
		}
		return mysql.MySQLFlavor, version, nil
	}
	for _, v := range mySQLSystemSettings {
		err = checkSystemSetting(c, v[0], v[1:])
		if err != nil {
			return "", "", err
		}
	}
	return mysql.MySQLFlavor, version, nil
}

// persistWALOffset loads an existing value from memo into walOffset or
// initializes to a user-provided value. It will also start a goroutine
// in the stopper to occasionally write an updated value back to the
// memo.
func (c *conn) persistWALOffset(ctx *stopper.Context) error {
	key := fmt.Sprintf("mysql-wal-offset-%s", c.target.Raw())
	found, err := c.memo.Get(ctx, c.stagingDB, key)
	if err != nil {
		return err
	}
	cp := newConsistentPoint(c.flavor)
	if len(found) > 0 {
		if _, err := cp.parseFrom(string(found)); err != nil {
			return err
		}
		c.walOffset.Set(cp)
	} else if c.config.InitialGTID != "" {
		// Set to a user-configured, default value.
		cp, err = cp.parseFrom(c.config.InitialGTID)
		if err != nil {
			return err
		}
	}
	c.walOffset.Set(cp)

	ctx.Go(func(ctx *stopper.Context) error {
		_, err := stopvar.DoWhenChanged(ctx, cp, &c.walOffset,
			func(ctx *stopper.Context, _, cp *consistentPoint) error {
				if err := c.memo.Put(ctx, c.stagingDB, key, []byte(cp.String())); err == nil {
					log.Tracef("stored WAL offset %s: %s", key, cp)
				} else {
					log.WithError(err).Warn("could not persist WAL offset")
				}
				return nil
			})
		return err
	})
	return nil
}

// checkSystemSetting verifies that the given system variable is set to one of
// the expected values.
func checkSystemSetting(c *client.Conn, variable string, expected []string) error {
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
	for _, e := range expected {
		if value == e {
			return nil
		}
	}
	return errors.Errorf("invalid server setting for %s. Expected one of: %s. Found %s",
		variable, strings.Join(expected, ","), value)
}
