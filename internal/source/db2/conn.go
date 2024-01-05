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
	"encoding/json"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/diag"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

// conn is responsible to pull the mutations from the DB2 staging tables
// and applying them to the target database via the logical.Batch interface.
// The process leverages the DB2 SQL replication, similar to
// the debezium DB2 connector.
// To set up DB2 for SQL replication, see the instructions at
// https://debezium.io/documentation/reference/stable/connectors/db2.html#setting-up-db2
// Note: In DB2 identifiers (e.g. table names, column names) are converted to
// uppercase, unless they are in quotes. The target schema must use the
// same convention.
type conn struct {
	columns     *ident.TableMap[[]types.ColData]
	primaryKeys *ident.TableMap[map[int]int]
	config      *Config
}

var (
	_ diag.Diagnostic = (*conn)(nil)
	_ logical.Dialect = (*conn)(nil)
)

// New instantiates a connection to the DB2 server.
func New(config *Config) logical.Dialect {
	return &conn{
		columns:     &ident.TableMap[[]types.ColData]{},
		primaryKeys: &ident.TableMap[map[int]int]{},
		config:      config,
	}
}

// Process implements logical.Dialect.
func (c *conn) Process(
	ctx *stopper.Context, ch <-chan logical.Message, events logical.Events,
) error {
	var startBatch time.Time
	var mutInBatch int
	var batch logical.Batch
	defer func() {
		log.Warn("db2.Process done. Rolling back")
		if batch != nil {
			rollbackBatchSize.Observe(float64(mutInBatch))
			_ = batch.OnRollback(ctx)
		}
	}()
	for msg := range ch {
		// Ensure that we resynchronize.
		if logical.IsRollback(msg) {
			if batch != nil {
				rollbackBatchSize.Observe(float64(mutInBatch))
				if err := batch.OnRollback(ctx); err != nil {
					return err
				}
				batch = nil
			}
			continue
		}

		ev, _ := msg.(message)
		switch ev.op {
		case beginOp:
			var err error
			batch, err = events.OnBegin(ctx)
			mutInBatch = 0
			startBatch = time.Now()
			if err != nil {
				return err
			}
		case endOp:
			select {
			case err := <-batch.OnCommit(ctx):
				if err != nil {
					return err
				}
				batchSize.Observe(float64(mutInBatch))
				batchLatency.Observe(float64(time.Since(startBatch).Seconds()))
				batch = nil
			case <-ctx.Done():
				return ctx.Err()
			}

			if err := events.SetConsistentPoint(ctx, &ev.lsn); err != nil {
				return err
			}
		case insertOp, updateOp, deleteOp:
			sourceTable := ident.NewTable(
				ident.MustSchema(ident.New(ev.table.sourceOwner)),
				ident.New(ev.table.sourceTable))
			targetTable := ident.NewTable(
				c.config.TargetSchema.Schema(),
				ident.New(ev.table.sourceTable))
			targetCols, ok := c.columns.Get(sourceTable)
			if !ok {
				return errors.Errorf("unable to retrieve target columns for %s", sourceTable)
			}
			primaryKeys, ok := c.primaryKeys.Get(sourceTable)
			if !ok {
				return errors.Errorf("unable to retrieve primary keys for %s", sourceTable)
			}
			var err error
			var mut types.Mutation
			key := make([]any, len(primaryKeys))
			enc := make(map[string]any)
			for idx, sourceCol := range ev.values {
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
			mut.Key, err = json.Marshal(key)
			if err != nil {
				return err
			}
			if ev.op != deleteOp {
				mut.Data, err = json.Marshal(enc)
				if err != nil {
					return err
				}
			}
			mutInBatch++
			mutationCount.With(prometheus.Labels{
				"table": sourceTable.Raw(),
				"op":    ev.op.String()}).Inc()
			log.Tracef("%s %s %s %v\n", ev.op, sourceTable.Raw(), targetTable.Raw(), key)
			script.AddMeta("db2", targetTable, &mut)
			err = batch.OnData(ctx, script.SourceName(targetTable), targetTable, []types.Mutation{mut})
			if err != nil {
				return err
			}
		default:
			log.Warnf("unknown op %v", ev.op)
		}
	}
	return nil
}

// ReadInto implements logical.Dialect.
func (c *conn) ReadInto(
	ctx *stopper.Context, ch chan<- logical.Message, state logical.State,
) error {
	db, err := c.Open()
	if err != nil {
		return errors.Wrap(err, "failed to open a connection to the target db")
	}
	defer func() {
		log.Warn("db2.ReadInto done. Closing database connection")
		err := db.Close()
		log.Info("Db closed.", err)
	}()
	cp, _ := state.GetConsistentPoint()
	if cp == nil {
		return errors.New("missing lsn")
	}
	previousLsn, _ := cp.(*lsn)
	for {
		nextLsn, err := c.getNextLsn(ctx, db, previousLsn)
		if err != nil {
			return errors.Wrap(err, "cannot retrieve next sequence number")
		}
		log.Debugf("NEXT  [%x]\n", nextLsn.Value)
		if nextLsn.Less(previousLsn) || nextLsn.Equal(previousLsn) || nextLsn.Equal(lsnZero()) {
			select {
			case <-time.After(1 * time.Second):
				continue
			case <-ctx.Stopping():
				return nil
			}
		}
		log.Debugf("BEGIN  [%x]\n", previousLsn.Value)
		select {
		case ch <- message{
			op:  beginOp,
			lsn: *nextLsn,
		}:
		case <-ctx.Stopping():
			return nil
		}

		lsnRange := lsnRange{
			from: previousLsn,
			to:   nextLsn,
		}
		tables, err := c.getTables(ctx, db, c.config.SourceSchema)
		if err != nil {
			log.Error("getTables failed", err)
			return errors.Wrap(err, "cannot get DB2 CDC tables")
		}
		for _, t := range tables {
			err := c.fetchColumnMetadata(ctx, db, &t)
			if err != nil {
				log.Error("fetchColumnMetadata failed", err)
				return errors.Wrap(err, "cannot retrieve colum names")
			}
		}

		for _, tbl := range tables {
			start := time.Now()
			count, err := c.postMutations(ctx, db, tbl, lsnRange, ch)
			if err != nil {
				log.Error("postMutations failed", err)
				return errors.Wrap(err, "cannot post mutation")
			}
			if ctx.IsStopping() {
				return nil
			}
			log.Debugf("post mutations for %s starting at %x. Count %d. Time %d.", tbl.sourceTable,
				lsnRange.from.Value, count,
				time.Since(start).Milliseconds())
			queryLatency.With(prometheus.Labels{
				"table": tbl.sourceTable}).Observe(time.Since(start).Seconds())
		}

		select {
		case ch <- message{
			op:  endOp,
			lsn: *nextLsn,
		}:
		case <-ctx.Stopping():
			return nil
		}
		log.Debugf("COMMIT [%x]\n", nextLsn.Value)
		previousLsn = nextLsn
	}

}

// ZeroStamp implements logical.Dialect.
func (c *conn) ZeroStamp() stamp.Stamp {
	return lsnZero()
}

// Diagnostic implements diag.Diagnostic.
func (c *conn) Diagnostic(_ context.Context) any {
	return map[string]any{
		"hostname":   c.config.host,
		"database":   c.config.database,
		"defaultLsn": c.config.DefaultConsistentPoint,
		"columns":    c.columns,
	}
}
