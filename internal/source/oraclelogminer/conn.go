// Copyright 2024 The Cockroach Authors
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

package oraclelogminer

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/diag"
	"github.com/cockroachdb/replicator/internal/util/ident"
	log "github.com/sirupsen/logrus"
)

type DB struct {
	*sql.DB

	// SCN is updated for each logMiner pull iteration.
	scn string

	// The destination for writes.
	acceptor types.MultiAcceptor
	// The connector configuration.
	config *Config

	// Persistent storage for WAL data.
	memo types.Memo

	// The destination for writes.
	target ident.Schema
	// Access to the target database.
	targetDB *types.TargetPool
}

//go:generate go run golang.org/x/tools/cmd/stringer -type=mutationType

var _ diag.Diagnostic = (*DB)(nil)

// Diagnostic implements [diag.Diagnostic].
// TODO(janexing): figure out the fields to fill in here.
func (db *DB) Diagnostic(_ context.Context) any {
	return map[string]any{}
}

func (db *DB) UpdateSCN(SCN string) {
	db.scn = SCN
}

func (db *DB) Start(ctx *stopper.Context) error {
	ctx.Go(func(ctx *stopper.Context) error {
		for !ctx.IsStopping() {
			if err := db.copyMessages(ctx); err != nil {
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

// copyMessages periodically pulls from the logMiner, parses the logs into mutations and apply
// the mutations on the target database.
func (db *DB) copyMessages(ctx *stopper.Context) error {
	// Create a ticker that triggers every second
	ticker := time.NewTicker(logMinerPullFrequency)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// TODO(janexing): make the changefeed acquiring executed IN PARALLEL TO the applying logic.
			logs, err := GetChangeFeedLogs(ctx, db)
			if err != nil {
				return errors.Wrapf(err, "failed to obtain changefeed logs")
			}
			// Logs with the same XID (transaction ID) will be put into the same temporalBatch.
			// TODO(janexing): set the commit SCN as the ext time for this temporal batch.
			temporalBatch := &types.TemporalBatch{}
			// Once all logs are processed, the batches are sent to conn acceptor one by one.
			temporalBatches := make([]*types.TemporalBatch, 0)

			var prevXID, currXID []byte
			for _, lg := range logs {
				// Parse the redo sql stmt to a kv struct.
				kv, err := LogToKV(lg.SqlRedo)
				if err != nil {
					return err
				}

				currXID = lg.TxnID
				if len(prevXID) == 0 {
					prevXID = lg.TxnID
				}

				// If the transaction ID changed, push the current temporal batch to the collection,
				// and create a new batch for the new transaction ID.
				if !bytes.Equal(currXID, prevXID) {
					temporalBatches = append(temporalBatches, temporalBatch)
					temporalBatch = &types.TemporalBatch{}
				}

				prevXID = lg.TxnID

				// We need to get the primary key values for the changefeed, as in the update or
				// delete stmt that logminer provides there might not explicitly contains the pk
				// values, but just the rowid.
				// TODO(janexing): consider the ordinal order of pks.
				if lg.Operation != Insert {
					pkNames, pkVals, err := RowIDToPKs(ctx, db.DB, lg.RowID, lg.UserName, lg.TableName, lg.SCN)
					if err != nil {
						return err
					}
					for i, name := range pkNames {
						if pkVals != nil {
							kv[name] = string(pkVals[i])
						}
					}
				}

				// Convert the kv struct into a mutation obj.
				byteRes, err := json.Marshal(kv)
				if err != nil {
					return errors.Wrapf(err, "failed to marshal kv")
				}
				mut := types.Mutation{
					Data:     byteRes,
					Deletion: lg.Operation == Delete,
				}

				rowIDRaw, err := json.Marshal(lg.RowID)
				if err != nil {
					return errors.Wrapf(err, "failed to marshal rowID")
				}
				mut.Key = rowIDRaw

				// Set the timestamp of mutation.
				// THE CURRENT IMPLEMENTATION HERE IS WRONG, as we don't have convenient way to
				// convert a SCN to a timestamp with sufficiently precision at this moment,
				// so we use this function as a placeholder for now.
				// TODO(janexing): use the commit SCN for ext for time.
				hlcTime, err := scnToHLCTime(lg.CommitSCN)
				if err != nil {
					return err
				}
				mut.Time = hlcTime

				if temporalBatch.Time.Nanos() == 0 {
					temporalBatch.Time = hlcTime
				}
				mut.SCN = lg.CommitSCN

				targetTbl := ident.NewTable(db.config.TargetSchema, ident.New(lg.TableName))
				if err := temporalBatch.Accumulate(targetTbl, mut); err != nil {
					return errors.Wrapf(err, "failed to accumulate mut to batch")
				}
			}

			if temporalBatch.Time.Nanos() != 0 {
				temporalBatches = append(temporalBatches, temporalBatch)
			}

			if len(temporalBatches) > 0 {
				log.Debug("started txn to apply temporal batches on target")

				tx, err := db.targetDB.BeginTx(ctx, &sql.TxOptions{})
				if err != nil {
					return errors.Wrapf(err, "failed to begin a txn on the target db")
				}

				log.Debug("start accepting temporal batches")

				for _, tmpBatch := range temporalBatches {
					if err := db.acceptor.AcceptTemporalBatch(ctx, tmpBatch, &types.AcceptOptions{
						TargetQuerier: tx,
					}); err != nil {
						return errors.Wrapf(err, "failed to accept temporal batch")
					}
				}

				log.Debug("finished accepting temporal batches")
				if err := tx.Commit(); err != nil {
					return err
				}
				log.Debug("txn committed")
			}

			log.Debugf("Next SCN:%s", db.scn)
		case <-ctx.Done():
			return nil
		}
	}
}
