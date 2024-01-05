// Copyright 2024 The Cockroach Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package db2

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/diag"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/notify"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/cockroachdb/cdc-sink/internal/util/stopvar"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/shopspring/decimal"
	log "github.com/sirupsen/logrus"
)

// Conn is responsible to pull the mutations from the DB2 staging tables
// and applying them to the target database via the logical.Batch interface.
// The process leverages the DB2 SQL replication, similar to
// the debezium DB2 connector.
// To set up DB2 for SQL replication, see the instructions at
// https://debezium.io/documentation/reference/stable/connectors/db2.html#setting-up-db2
// Note: In DB2 identifiers (e.g. table names, column names) are converted to
// uppercase, unless they are in quotes. The target schema must use the
// same convention.
type Conn struct {
	// The destination for writes.
	acceptor types.MultiAcceptor
	// The database connection.
	db *sql.DB
	// Memoize column metadata for each table.
	columns *ident.TableMap[[]types.ColData]
	// Configuration of the connector.
	config *Config
	// Memoize of primary keys position for each table.
	// Maps the column, based on the order of ColData, to its position in the primary key.
	primaryKeys *ident.TableMap[map[int]int]
	// Memoize queries to check SQL replication.
	replQueries map[string]string
	// Access to the staging cluster.
	stagingDB *types.StagingPool
	// Persistent storage for WAL data.
	memo types.Memo
	// Memoization of queries to fetch change events.
	tableQueries *ident.TableMap[string]
	// The destination for writes.
	target ident.Schema
	// Access to the target database.
	targetDB *types.TargetPool
	// Consistent Point, managed by persistWALOffset.
	walOffset notify.Var[*lsn]
}

var (
	_ diag.Diagnostic = (*Conn)(nil)
)

// Start the DB2 connector.
func (c *Conn) Start(ctx *stopper.Context) error {
	// Call this first to load the previous offset. We want to reset our
	// state before starting the main copier routine.
	if err := c.persistWALOffset(ctx); err != nil {
		return err
	}

	// Start a process to copy data to the target.
	ctx.Go(func() error {
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

// Diagnostic implements diag.Diagnostic.
func (c *Conn) Diagnostic(_ context.Context) any {
	return map[string]any{
		"hostname":   c.config.host,
		"database":   c.config.database,
		"defaultLsn": c.config.InitialLSN,
		"columns":    c.columns,
	}
}

// accumulateBatch finds all the rows that have been changed on the given table
// and adds them to the batch.
func (c *Conn) accumulateBatch(
	ctx *stopper.Context, sourceTable ident.Table, lsnRange lsnRange, batch *types.MultiBatch,
) (int, error) {
	count := 0
	query, ok := c.tableQueries.Get(sourceTable)
	if !ok {
		return 0, errors.Errorf("unknown table %q", sourceTable)
	}
	// TODO (silvano): we might want to run in batches with a limit.
	rows, err := c.db.QueryContext(ctx, query, lsnRange.From.Value[:], lsnRange.To.Value[:])
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	cols, err := rows.ColumnTypes()
	if err != nil {
		return 0, err
	}
	// the first four columns are metadata (opcode, commit_seq, intent_seq, op_id)
	numMetaColumns := 5
	numCols := len(cols)
	if numCols <= numMetaColumns {
		return 0, errors.Errorf("error in query template. Not enough columns. %q", query)
	}
	dataCols := cols[numMetaColumns:]
	for rows.Next() {
		count++
		res := make([]any, numCols)
		ptr := make([]any, numCols)
		for i := range cols {
			ptr[i] = &res[i]
		}
		err = rows.Scan(ptr...)
		if err != nil {
			return count, err
		}
		// first column is the  opcode
		opcode, ok := res[0].(int32)
		if !ok || !validOperation(opcode) {
			return count, errors.Errorf("invalid operation  %T", res[0])
		}
		op := operation(opcode)
		// second column is a timestamp
		ts, ok := res[1].(time.Time)
		if !ok {
			return count, errors.Errorf("invalid timestamp %v", res[1])
		}
		values := make([]any, len(res)-numMetaColumns)
		for i, v := range res[numMetaColumns:] {
			if v == nil {
				values[i] = nil
				continue
			}
			switch dataCols[i].DatabaseTypeName() {
			case "VARCHAR", "CHAR", "CLOB":
				s, ok := v.([]byte)
				if !ok {
					return count, errors.Errorf("invalid type %T, expected []byte", s)
				}
				values[i] = string(s)
			case "DECIMAL", "DECFLOAT":
				s, ok := v.([]byte)
				if !ok {
					return count, errors.Errorf("invalid type %T, expected []byte", s)
				}
				values[i], err = decimal.NewFromString(string(s))
				if err != nil {
					errors.Wrapf(err, "cannot convert decimal %s", s)
					return count, err
				}
			default:
				values[i] = v
			}
		}
		targetTable := ident.NewTable(
			c.target,
			ident.New(sourceTable.Table().Raw()))
		targetCols, ok := c.columns.Get(sourceTable)
		if !ok {
			return 0, errors.Errorf("unable to retrieve target columns for %s", sourceTable)
		}
		primaryKeys, ok := c.primaryKeys.Get(sourceTable)
		if !ok {
			return 0, errors.Errorf("unable to retrieve primary keys for %s", sourceTable)
		}
		key := make([]any, len(primaryKeys))
		enc := make(map[string]any)
		for idx, sourceCol := range values {
			targetCol := targetCols[idx]
			switch s := sourceCol.(type) {
			case nil:
				enc[targetCol.Name.Raw()] = nil
			default:
				enc[targetCol.Name.Raw()] = s
			}
			if targetCol.Primary {
				key[primaryKeys[idx]] = sourceCol
			}
		}
		var mut types.Mutation
		mut.Time = hlc.New(ts.UnixNano(), 0)
		if mut.Key, err = json.Marshal(key); err != nil {
			return count, err
		}
		if op != deleteOp {
			mut.Data, err = json.Marshal(enc)
			if err != nil {
				return count, err
			}
		}
		mutationCount.With(prometheus.Labels{
			"table": sourceTable.Raw(),
			"op":    op.String()}).Inc()
		log.Infof("(%s,%v) %s => %s \n", op, key, sourceTable.Raw(), targetTable.Raw())

		script.AddMeta("db2", targetTable, &mut)
		if err = batch.Accumulate(targetTable, mut); err != nil {
			return count, err
		}
	}
	return count, nil
}

// copyMessages read messages from the DB2 staging tables,
// accumulates them, and commits data to the target
func (c *Conn) copyMessages(ctx *stopper.Context) error {
	var err error
	c.db, err = c.open()
	if err != nil {
		return errors.Wrap(err, "failed to open a connection to the target db")
	}
	defer func() {
		err := c.db.Close()
		if err != nil {
			log.Errorf("Error closing the database. %q", err)
		}
	}()
	previousLsn, _ := c.walOffset.Get()
	if previousLsn == nil {
		return errors.New("missing log sequence number")
	}
	log.Infof("DB2 connector starting at %x", previousLsn.Value)
	for {
		nextLsn, err := c.getNextLsn(ctx, previousLsn)
		if err != nil {
			return errors.Wrap(err, "cannot retrieve next sequence number")
		}
		log.Tracef("NEXT  [%x]\n", nextLsn.Value)
		if nextLsn.Less(previousLsn) || nextLsn.Equal(previousLsn) || nextLsn.Equal(lsnZero()) {
			select {
			case <-time.After(c.config.PollingInterval):
				continue
			case <-ctx.Stopping():
				return nil
			}
		}
		start := time.Now()
		log.Tracef("BEGIN  [%x]\n", previousLsn.Value)
		tables, stagingTables, err := c.getTables(ctx, c.config.SourceSchema)
		if err != nil {
			return errors.Wrap(err, "cannot get DB2 CDC tables")
		}
		currentLsnRange := lsnRange{
			From: previousLsn,
			To:   nextLsn,
		}
		batch := &types.MultiBatch{}
		for idx, table := range tables {
			start := time.Now()
			stagingTable := stagingTables[idx]
			err := c.populateTableMetadata(ctx, table, stagingTable)
			if err != nil {
				return errors.Wrap(err, "cannot retrieve table metadata")
			}
			count, err := c.accumulateBatch(ctx, table, currentLsnRange, batch)
			if err != nil {
				return errors.Wrap(err, "cannot post mutation")
			}
			if ctx.IsStopping() {
				return nil
			}
			log.Tracef("post mutations for %s starting at %x. Count %d. Time %d.", table,
				currentLsnRange.From.Value, count,
				time.Since(start).Milliseconds())
			queryLatency.With(prometheus.Labels{
				"table": table.Canonical().Raw()}).Observe(time.Since(start).Seconds())
		}
		if batch.Count() == 0 {
			log.Trace("skipping empty transaction")
		} else {
			log.Debugf("COMMIT [%x]-[%x]\n", previousLsn.Value, nextLsn.Value)
			if err := c.commit(ctx, batch); err != nil {
				return errors.WithStack(err)
			}
		}
		batchLatency.Observe(float64(time.Since(start).Seconds()))
		previousLsn = nextLsn
		c.walOffset.Set(previousLsn)
	}
}

// commit the current batch to the target database.
func (c *Conn) commit(ctx *stopper.Context, batch *types.MultiBatch) error {
	batchSize.Observe(float64(batch.Count()))
	tx, err := c.targetDB.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return errors.WithStack(err)
	}
	defer tx.Rollback()
	if err := c.acceptor.AcceptMultiBatch(ctx, batch, &types.AcceptOptions{
		TargetQuerier: tx,
	}); err != nil {
		return errors.WithStack(err)
	}
	return tx.Commit()
}

// getFirstLsnFound returns the first row that contains a log sequence number.
func getFirstLsnFound(rows *sql.Rows) (*lsn, error) {
	if rows.Next() {
		var v []byte
		err := rows.Scan(&v)
		if err != nil {
			return nil, err
		}
		return newLSN(v)
	}
	return lsnZero(), nil
}

// getLsnFromMonitoring retrieves the sequence number from
// the monitoring table
func (c *Conn) getLsnFromMonitoring(ctx *stopper.Context, current *lsn) (*lsn, error) {
	// Checking the monitoring table for a batch of mutations.
	rows, err := c.db.QueryContext(ctx, c.replQueries["nextLsn"], current.Value[:])
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return getFirstLsnFound(rows)
}

// getMaxLsn retrieves the latest sequence number in all the DB2 staging tables.
func (c *Conn) getMaxLsn(ctx *stopper.Context) (*lsn, error) {
	rows, err := c.db.QueryContext(ctx, c.replQueries["maxLsn"])
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return getFirstLsnFound(rows)
}

// getNextLsn retrieves the next sequence number
func (c *Conn) getNextLsn(ctx *stopper.Context, current *lsn) (*lsn, error) {
	// Checking the monitoring table first, maybe we can process a smaller batch.
	lsn, err := c.getLsnFromMonitoring(ctx, current)
	if err != nil {
		return nil, err
	}
	// If we don't find a lsn in the monitoring table, then look in the
	// DB2 staging tables.
	if lsn.Equal(lsnZero()) {
		return c.getMaxLsn(ctx)
	}
	return lsn, nil
}

// getTables returns the source tables and corresponding staging tables in DB2.
// The DB2 staging tables store the committed transactional data
// for all the change events tracked by the SQL replication in the source database.
func (c *Conn) getTables(
	ctx *stopper.Context, schema ident.Schema,
) ([]ident.Table, []ident.Table, error) {
	var rows *sql.Rows
	var err error
	if schema.Empty() {
		rows, err = c.db.QueryContext(ctx, c.replQueries["stagingTables"])
	} else {
		rows, err = c.db.QueryContext(ctx, c.replQueries["stagingTablesForSchema"], schema.Raw())
	}
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	tables := make([]ident.Table, 0)
	stagingTables := make([]ident.Table, 0)
	for rows.Next() {
		var sourceSchema, sourceTable string
		var stagingSchema, stagingTable string
		err = rows.Scan(&sourceSchema, &sourceTable, &stagingSchema, &stagingTable)
		if err != nil {
			return nil, nil, err
		}
		tables = append(tables, ident.NewTable(
			ident.MustSchema(ident.New(sourceSchema)),
			ident.New(sourceTable)))

		stagingTables = append(stagingTables, ident.NewTable(
			ident.MustSchema(ident.New(stagingSchema)),
			ident.New(stagingTable)))
	}
	return tables, stagingTables, nil
}

// open a new connection to the source database.
func (c *Conn) open() (*sql.DB, error) {
	con := fmt.Sprintf("HOSTNAME=%s;DATABASE=%s;PORT=%d;UID=%s;PWD=%s",
		c.config.host,
		c.config.database,
		c.config.port,
		c.config.user,
		c.config.password)
	log.Debugf("DB2 connection: HOSTNAME=%s;DATABASE=%s;PORT=%d;UID=%s;PWD=...",
		c.config.host,
		c.config.database,
		c.config.port,
		c.config.user)

	return sql.Open("go_ibm_db", con)
}

// persistLsn loads an existing value from memo into WALOffset or
// initializes to a user-provided value. It will also start a goroutine
// in the stopper to occasionally write an updated value back to the
// memo.
func (c *Conn) persistWALOffset(ctx *stopper.Context) error {
	key := fmt.Sprintf("db2-lsn-%s", c.target.Raw())
	found, err := c.memo.Get(ctx, c.stagingDB, key)
	if err != nil {
		return err
	}
	cp := &lsn{}
	if len(found) > 0 {
		if err := cp.UnmarshalText(found); err != nil {
			return err
		}
	} else if c.config.InitialLSN != "" {
		if err := cp.UnmarshalText([]byte(c.config.InitialLSN)); err != nil {
			return err
		}
	}
	c.walOffset.Set(cp)
	ctx.Go(func() error {
		_, err := stopvar.DoWhenChanged(ctx, cp, &c.walOffset,
			func(ctx *stopper.Context, _, cp *lsn) error {
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

// populateTableMetadata fetches the column metadata for a given table
// and caches it.
func (c *Conn) populateTableMetadata(
	ctx *stopper.Context, sourceTable ident.Table, stagingTable ident.Table,
) error {
	if _, ok := c.columns.Get(sourceTable); ok {
		return nil
	}
	// to get the changes for the given source table, we need to query the staging table.
	c.tableQueries.Put(sourceTable,
		fmt.Sprintf(changeQuery, stagingTable, c.config.SQLReplicationSchema))
	// get the columns and primary keys.
	rows, err := c.db.QueryContext(ctx, columnQuery, sourceTable.Schema().Raw(), sourceTable.Table().Raw())
	if err != nil {
		return err
	}
	defer rows.Close()
	colData := make([]types.ColData, 0)
	primaryKeys := make(map[int]int)
	idx := 0
	for rows.Next() {
		var name, typ string
		var pk *int
		err = rows.Scan(&name, &pk, &typ)
		if err != nil {
			return err
		}
		if pk != nil {
			primaryKeys[idx] = *pk - 1
		}
		log.Tracef("Table %s col %q %q primary?: %t", sourceTable, name, typ, pk != nil)
		colData = append(colData, types.ColData{
			Name:    ident.New(name),
			Primary: pk != nil,
			Type:    typ,
		})
		idx++
	}
	c.columns.Put(sourceTable, colData)
	c.primaryKeys.Put(sourceTable, primaryKeys)
	return nil
}
