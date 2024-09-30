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
	"encoding/json"
	"time"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/source/oraclelogminer/scn"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/diag"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// DB wraps the connection pool to the oracle source with applicator-related structs to the target.
type DB struct {
	*types.SourcePool

	// The connector configuration.
	config *Config

	// Ensure the timestamps we generate always march forward.
	monotonic hlc.Clock

	// Access to the target database.
	targetDB *types.TargetPool

	// Holds the guaranteed-committed SCN.
	walOffset notify.Var[*scn.SCN]
}

var _ diag.Diagnostic = (*DB)(nil)

// Diagnostic implements [diag.Diagnostic].
// TODO(janexing): figure out the fields to fill in here.
func (db *DB) Diagnostic(_ context.Context) any {
	return map[string]any{}
}

// outputMessage periodically pulls from the logMiner, parses the logs
// into mutations and batches the mutations based on their transaction
// ID, and push it to the out channel. All mutations that belong to the
// same source transaction will be grouped into the same temporal batch.
func (db *DB) outputMessage(ctx *stopper.Context, out chan<- *types.BatchCursor) {
	// Create a ticker that triggers every interval.
	ticker := time.NewTicker(db.config.LogMinerPullInterval)
	defer ticker.Stop()

	startSCN := db.config.SCN
	var err error

	if startSCN.IsEmpty() {
		startSCN, err = getLatestSCN(ctx, db.SourcePool)
		if err != nil {
			out <- &types.BatchCursor{Error: errors.Wrapf(err, "failed to obtained latest SCN")}
			return
		}
		log.Warnf("initial SCN not specified, using auto-queried SCN %s", startSCN)
	}

	// Pull based model for acquiring the changefeed logs and convert them into mutations.
	for {
		select {
		case <-ticker.C:
			// TODO(janexing): make the changefeed acquiring executed IN PARALLEL TO the applying logic.
			logs, nextStartSCN, err := readLogsOnce(ctx, db.SourcePool, startSCN, db.config.SourceSchema.Raw())
			if err != nil {
				out <- &types.BatchCursor{Error: errors.Wrapf(err, "failed to obtain changefeed logs")}
				return
			}
			startSCN = nextStartSCN
			log.Infof("received %d logs", len(logs))
			// Logs with the same XID (transaction ID) will be put into the same temporalBatch.
			// TODO(janexing): set the commit SCN as the ext time for this temporal batch.
			temporalBatch := &types.TemporalBatch{}

			var prevXID, currXID []byte
			for _, lg := range logs {
				// Parse the redo sql stt to a kv struct.
				kv, err := LogToKV(lg.SQLRedo)
				if err != nil {
					out <- &types.BatchCursor{Error: err}
					return
				}

				currXID = lg.TxnID
				if len(prevXID) == 0 {
					prevXID = lg.TxnID
				}

				// If the transaction ID changed, push the current temporal batch to the collection,
				// and create a new batch for the new transaction ID.
				if !bytes.Equal(currXID, prevXID) {
					// TODO(janexing): figure out what else to include in the batch cursor.
					out <- &types.BatchCursor{
						Batch:    temporalBatch,
						Progress: hlc.RangeIncluding(hlc.Zero(), temporalBatch.Time),
					}
					temporalBatch = &types.TemporalBatch{}
				}

				prevXID = lg.TxnID

				// We need to get the primary key values for the changefeed, as in the update or
				// delete stmt that logminer provides there might not explicitly contains the pk
				// values, but just the rowid.
				actualKV := kv.SetKV

				if lg.Operation != OperationInsert {
					//pkNames, err := GetPKNames(ctx, db.SourcePool, lg.SegOwner, lg.TableName)
					//if err != nil {
					//	out <- &types.BatchCursor{Error: err}
					//	return
					//}
					//// TODO(janexing): consider changefeed log where the PK is updated.
					//for _, pkName := range pkNames {
					//	pkVal, ok := kv.WhereKV[pkName]
					//	if !ok {
					//		out <- &types.BatchCursor{Error: errors.Errorf("value not found for primary key %s in the changefeed log", pkName)}
					//		return
					//	}
					//	kv.SetKV[pkName] = pkVal
					//}
					actualKV = kv.MergeSetAndWhere()
				}

				// Convert the kv struct into a mutation obj.
				byteRes, err := json.Marshal(actualKV)
				if err != nil {
					out <- &types.BatchCursor{Error: errors.Wrapf(err, "failed to marshal kv")}
					return
				}
				mut := types.Mutation{
					Data:     byteRes,
					Deletion: lg.Operation == OperationDelete,
				}

				rowIDRaw, err := json.Marshal(lg.RowID)
				if err != nil {
					out <- &types.BatchCursor{Error: errors.Wrapf(err, "failed to marshal rowID")}
					return
				}
				mut.Key = rowIDRaw

				// TODO(janexing): we only set the external of time for
				// mutation and temporal batch, without taking care of
				// nano and logical. Is this sufficient?
				if temporalBatch.Time.External() == nil {
					commitSCN, err := scn.ParseStringToSCN(lg.CommitSCN)
					if err != nil {
						out <- &types.BatchCursor{Error: errors.Wrapf(err, "failed to parse commit SCN for log")}
					}
					temporalBatch.Time = db.monotonic.External(commitSCN)
				}
				mut.Time = temporalBatch.Time

				targetTbl := ident.NewTable(db.config.TargetSchema, ident.New(lg.TableName))
				if err := temporalBatch.Accumulate(targetTbl, mut); err != nil {
					out <- &types.BatchCursor{Error: errors.Wrapf(err, "failed to accumulate mutation to batch")}
					return
				}
			}

			// TODO(janexing): figure out what else to include in the batch cursor.
			if temporalBatch.Count() > 0 {
				out <- &types.BatchCursor{
					Batch:    temporalBatch,
					Progress: hlc.RangeIncluding(hlc.Zero(), temporalBatch.Time),
				}
			}

		case <-ctx.Done():
			return
		}
	}
}

var _ types.BatchReader = (*DB)(nil)

func (db *DB) Read(ctx *stopper.Context) (<-chan *types.BatchCursor, error) {
	readChan := make(chan *types.BatchCursor, 2)

	ctx.Go(func(ctx *stopper.Context) error {
		db.outputMessage(ctx, readChan)
		return nil
	})
	return readChan, nil
}
