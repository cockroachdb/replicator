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
	"database/sql"
	"fmt"
	"strings"

	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	log "github.com/sirupsen/logrus"
)

var (

	// maxLsn is the max Log Sequence Number stored in the staging tables.
	// This is used during live capture of events
	maxLsn = `SELECT max(t.SYNCHPOINT) 
	   FROM ( SELECT CD_NEW_SYNCHPOINT AS SYNCHPOINT FROM 
	        ASNCDC.IBMSNAP_REGISTER UNION ALL SELECT SYNCHPOINT AS SYNCHPOINT FROM ASNCDC.IBMSNAP_REGISTER) t
	`

	// nexLsn is the Log Sequence Number of the next batch from monitoring table.
	// This is used during catch up, to batch mutations.
	// The monitoring table is refreshed periodically based on the MONITOR_INTERVAL parameter as
	// set it the ASNCDC.IBMSNAP_CAPPARMS table. By default, it set to 300 seconds.
	nextLsn = `
	select MIN(RESTART_SEQ) from ASNCDC.IBMSNAP_CAPMON where CD_ROWS_INSERTED > 0 AND RESTART_SEQ > ?
	`

	// get mutations
	db2CdcTables = `
	     SELECT r.SOURCE_OWNER, r.SOURCE_TABLE, 
		         r.CD_OWNER, r.CD_TABLE,
				 r.CD_NEW_SYNCHPOINT, r.CD_OLD_SYNCHPOINT, 
				 t.TBSPACEID, t.TABLEID 
		 FROM 
            ASNCDC.IBMSNAP_REGISTER r 
			LEFT JOIN SYSCAT.TABLES t 
				ON r.SOURCE_OWNER  = t.TABSCHEMA 
				AND r.SOURCE_TABLE = t.TABNAME  
			WHERE r.SOURCE_OWNER <> ''
			`
	// get mutations for a schema
	db2CdcTablesForSchema = `
		SELECT r.SOURCE_OWNER, r.SOURCE_TABLE, 
				r.CD_OWNER, r.CD_TABLE,
				r.CD_NEW_SYNCHPOINT, r.CD_OLD_SYNCHPOINT, 
				t.TBSPACEID, t.TABLEID 
		FROM 
		   ASNCDC.IBMSNAP_REGISTER r 
		   LEFT JOIN SYSCAT.TABLES t 
			   ON r.SOURCE_OWNER  = t.TABSCHEMA 
			   AND r.SOURCE_TABLE = t.TABNAME  
		   WHERE r.SOURCE_OWNER = ?
		   `
	// Note: we skip deletes before updates.
	changeQuery = `
	SELECT * FROM 
		(SELECT CASE
		WHEN IBMSNAP_OPERATION = 'D' AND (LEAD(cdc.IBMSNAP_OPERATION,1,'X') OVER (PARTITION BY cdc.IBMSNAP_COMMITSEQ ORDER BY cdc.IBMSNAP_INTENTSEQ)) ='I' THEN 0
		WHEN IBMSNAP_OPERATION = 'I' AND (LAG(cdc.IBMSNAP_OPERATION,1,'X') OVER (PARTITION BY cdc.IBMSNAP_COMMITSEQ ORDER BY cdc.IBMSNAP_INTENTSEQ)) ='D' THEN 3
		WHEN IBMSNAP_OPERATION = 'D' THEN 1
		WHEN IBMSNAP_OPERATION = 'I' THEN 2
		END
		OPCODE,
		cdc.*
		FROM  # cdc WHERE   IBMSNAP_COMMITSEQ > ? AND IBMSNAP_COMMITSEQ <= ? 
		order by IBMSNAP_COMMITSEQ, IBMSNAP_INTENTSEQ)
	WHERE OPCODE != 0	
	`
	columnQuery = `
	 SELECT c.colname,c.keyseq, c.TYPENAME  
            FROM syscat.tables as t 
            inner join syscat.columns as c  
			      on t.tabname = c.tabname 
			      and t.tabschema = c.tabschema 
     WHERE t.tabschema = ? AND t.tabname = ?
	 ORDER by c.COLNO
	`
)

type cdcTable struct {
	sourceOwner string
	sourceTable string
	cdOwner     string
	cdTable     string
	cdNewSyc    []byte
	cdOldSyc    []byte
	spaceID     int
	tableID     int
}

type message struct {
	table  cdcTable
	op     operation
	lsn    lsn
	values []any
}

//go:generate go run golang.org/x/tools/cmd/stringer -type=operation

type operation int

const (
	unknownOp operation = iota
	deleteOp
	insertOp
	updateOp
	beginOp
	endOp
	rollbackOp
)

// Open a new connection to the source database.
func (c *conn) Open() (*sql.DB, error) {
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

// postMutations finds all the rows that have been changed on the given table
// and posts them to the channel used by the logical loop to be processed.
func (c *conn) postMutations(
	ctx *stopper.Context, db *sql.DB, cdcTable cdcTable, lsnRange lsnRange, ch chan<- logical.Message,
) (int, error) {
	count := 0
	// TODO (silvano): memoize, use templates
	table := ident.NewTable(ident.MustSchema(ident.New(cdcTable.cdOwner)), ident.New(cdcTable.cdTable))
	query := strings.ReplaceAll(changeQuery, "#", table.Raw())
	// TODO (silvano): we might want to run in batches with a limit.
	rows, err := db.QueryContext(ctx, query, lsnRange.from.Value, lsnRange.to.Value)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	cols, _ := rows.ColumnTypes()
	// the first four columns are metadata (opcode, commit_seq, intent_seq, op_id)
	// the remaining columns contain the
	numMetaColums := 4
	for rows.Next() {
		count++
		mut := len(cols)
		res := make([]any, mut)
		ptr := make([]any, mut)
		for i := range cols {
			ptr[i] = &res[i]
		}
		err = rows.Scan(ptr...)
		if err != nil {
			log.Fatal(err)
		}
		msg := message{
			table: cdcTable,
		}
		// first column is the  opcode
		op, ok := res[0].(int32)
		if !ok {
			return count, errors.Errorf("invalid operation  %T", res[0])
		}
		msg.op = operation(op)

		// second column is the LSN of the committed transaction.
		cs, ok := res[1].([]byte)
		if !ok {
			return count, errors.Errorf("invalid commit sequence %v", res[1])
		}
		msg.lsn = lsn{Value: cs}

		msg.values = make([]any, len(res)-4)
		vcols := cols[numMetaColums:]
		for i, v := range res[numMetaColums:] {
			if v == nil {
				msg.values[i] = nil
				continue
			}
			// TODO (silvano): handle all the types
			// https://www.ibm.com/docs/en/db2-for-zos/13?topic=elements-data-types
			switch vcols[i].DatabaseTypeName() {
			case "VARCHAR", "CHAR", "CLOB":
				s, _ := v.([]byte)
				msg.values[i] = string(s)
			case "DECIMAL", "DECFLOAT":
				s, _ := v.([]byte)
				msg.values[i], err = decimal.NewFromString(string(s))
				if err != nil {
					errors.Wrapf(err, "cannot convert decimal %s", s)
					return count, err
				}
			default:
				msg.values[i] = v
			}
		}
		select {
		case ch <- msg:
		case <-ctx.Stopping():
			return -1, nil
		}
	}
	return count, nil
}

// getMaxLsn retrieves the latest sequence number in all the staging tables.
func (c *conn) getMaxLsn(ctx *stopper.Context, db *sql.DB) (*lsn, error) {
	rows, err := db.QueryContext(ctx, maxLsn)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if rows.Next() {
		var v []byte
		err = rows.Scan(&v)
		if err == nil {
			if len(v) == 0 {
				return lsnZero(), nil
			}
			return &lsn{
				Value: v,
			}, err
		}
		return nil, err
	}
	return lsnZero(), nil
}

// getNextLsn retrieves the next sequence number
func (c *conn) getNextLsn(ctx *stopper.Context, db *sql.DB, current *lsn) (*lsn, error) {
	// Checking the monitoring table for a batch of mutations.
	rows, err := db.QueryContext(ctx, nextLsn, current.Value)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if rows.Next() {
		var v []byte
		err = rows.Scan(&v)
		if err == nil {
			// if we don't have a LSN from the monitoring table,
			// we can check the staging tables directly.
			if len(v) == 0 {
				return c.getMaxLsn(ctx, db)
			}
			return &lsn{
				Value: v,
			}, err
		}
		return nil, err
	}
	return lsnZero(), nil
}

// fetchColumnMetadata fetches the column metadata for a given table.
func (c *conn) fetchColumnMetadata(ctx *stopper.Context, db *sql.DB, t *cdcTable) error {
	sourceTable := ident.NewTable(
		ident.MustSchema(ident.New(t.sourceOwner)),
		ident.New(t.sourceTable))

	if _, ok := c.columns.Get(sourceTable); ok {
		return nil
	}
	rows, err := db.QueryContext(ctx, columnQuery, sourceTable.Schema().Raw(), sourceTable.Table().Raw())
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
			primaryKeys[*pk] = idx
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

func queryTables(ctx *stopper.Context, db *sql.DB, schema ident.Schema) (*sql.Rows, error) {
	if schema.Empty() {
		return db.QueryContext(ctx, db2CdcTables)
	}
	return db.QueryContext(ctx, db2CdcTablesForSchema, schema.Raw())
}

// getTables returns the tables that have replication enabled.
func (c *conn) getTables(
	ctx *stopper.Context, db *sql.DB, schema ident.Schema,
) ([]cdcTable, error) {
	rows, err := queryTables(ctx, db, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]cdcTable, 0)
	for rows.Next() {
		var res cdcTable
		err = rows.Scan(&res.sourceOwner,
			&res.sourceTable,
			&res.cdOwner,
			&res.cdTable,
			&res.cdNewSyc,
			&res.cdOldSyc,
			&res.spaceID,
			&res.tableID,
		)
		if err != nil {
			return nil, err
		}
		result = append(result, res)
	}
	return result, nil
}
