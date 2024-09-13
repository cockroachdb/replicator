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

package oraclelogminer

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"os"
	"time"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/script"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/sequencer/chaos"
	"github.com/cockroachdb/replicator/internal/sequencer/immediate"
	scriptSeq "github.com/cockroachdb/replicator/internal/sequencer/script"
	"github.com/cockroachdb/replicator/internal/target/apply"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/godror/godror"
	"github.com/google/wire"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideDB,
	ProvideEagerConfig,
)

const (
	logModeCheckStmt = `select log_mode from v$database`
	archiveLog       = `ARCHIVELOG`
)
const supplementalLogDataStmt = `select supplemental_log_data_min from v$database`

// OCIPathEnvVar is the path where the Oracle Instant Client library is stored.
const OCIPathEnvVar = `OIC_LIBRARY_PATH`

const logMinerPullFrequency = 300 * time.Millisecond

func ProvideDB(
	ctx *stopper.Context,
	acc *apply.Acceptor,
	chaos *chaos.Chaos,
	config *Config,
	imm *immediate.Immediate,
	scriptSeq *scriptSeq.Sequencer,
	targetPool *types.TargetPool,
	watchers types.Watchers,
) (*DB, error) {
	if err := config.Preflight(); err != nil {
		return nil, err
	}
	params, err := godror.ParseDSN(config.SourceConn)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse the source connection string for parameters")
	}
	// Use go's pool, instead of the C library's pool.
	params.StandaloneConnection = true
	// If unset, the driver would otherwise use the local system timezone.
	if params.Timezone == nil {
		params.Timezone = time.UTC
	}

	if ociLibPath := os.Getenv(OCIPathEnvVar); ociLibPath != "" {
		params.LibDir = ociLibPath
	}

	connector := godror.NewConnector(params)
	godrorDB := sql.OpenDB(connector)

	if err := godrorDB.Ping(); err != nil {
		return nil, errors.Wrapf(err, "failed pinging oracle db")
	}

	if err := logMnrEnabledCheck(ctx, godrorDB); err != nil {
		return nil, errors.Wrapf(err, "failed to start replicator for oracle source")
	}

	schema := config.TargetSchema

	db := &DB{ConnStr: config.SourceConn, DB: godrorDB, UserName: params.Username}
	if config.SCN != "" {
		db.SCN = config.SCN
	}

	// Create a ticker that triggers every second
	ticker := time.NewTicker(logMinerPullFrequency)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// TODO(janexing): make the changefeed acquiring executed IN PARALLEL TO the applying logic.
			logs, err := GetChangeFeedLogs(ctx, db, config.TableUser)

			// Logs with the same XID (transaction ID) will be put into the same temporalBatch.
			temporalBatch := &types.TemporalBatch{}
			// Once all logs are processed, the batches are sent to conn acceptor one by one.
			temporalBatches := make([]*types.TemporalBatch, 0)

			var prevXID, currXID []byte
			for _, lg := range logs {
				// Parse the redo sql stmt to a kv struct.
				kv, err := LogToKV(lg.SqlRedo)
				if err != nil {
					return nil, err
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

				// We need to get the primary key values for the changefeed, as in the update stmt
				// that logminer provides there might not explicitly contains the pk values, but just
				// the rowid.
				// TODO(janexing): consider the ordinal order of pks.
				if lg.Operation != Insert {
					pkNames, pkVals, err := RowIDToPKs(ctx, godrorDB, lg.RowID, lg.UserName, lg.TableName, lg.SCN)
					if err != nil {
						return nil, err
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
					return db, errors.Wrapf(err, "failed to marshal kv")
				}
				mut := types.Mutation{Data: byteRes}

				rowIDRaw, err := json.Marshal(lg.RowID)
				if err != nil {
					return db, errors.Wrapf(err, "failed to marshal rowID")
				}
				mut.Key = rowIDRaw

				// Set the timestamp of mutation.
				// THE CURRENT IMPLEMENTATION HERE IS WRONG, as we don't have convenient way to
				// convert a SCN to a timestamp with sufficiently precision at this moment,
				// so we use this function as a placeholder for now.
				hlcTime, err := scnToHLCTime(lg.CommitSCN)
				if err != nil {
					return db, err
				}
				mut.Time = hlcTime

				if temporalBatch.Time.Nanos() == 0 {
					temporalBatch.Time = hlcTime
				}
				mut.SCN = lg.CommitSCN

				targetTbl := ident.NewTable(schema, ident.New(lg.TableName))
				if err := temporalBatch.Accumulate(targetTbl, mut); err != nil {
					return nil, errors.Wrapf(err, "failed to accumulate mut to batch")
				}
			}

			if temporalBatch.Time.Nanos() != 0 {
				temporalBatches = append(temporalBatches, temporalBatch)
			}

			// Change core seq to accept chan of mut, rather than chan of batch
			seq, err := scriptSeq.Wrap(ctx, imm)
			if err != nil {
				return nil, err
			}
			seq, err = chaos.Wrap(ctx, seq) // No-op if probability is 0.
			if err != nil {
				return nil, err
			}

			connAcceptor, _, err := seq.Start(ctx, &sequencer.StartOptions{
				Delegate: types.OrderedAcceptorFrom(acc, watchers),
				Bounds:   &notify.Var[hlc.Range]{}, // Not currently used.
				Group: &types.TableGroup{
					Name:      ident.New(config.TargetSchema.Raw()),
					Enclosing: config.TargetSchema,
				},
			})

			if err != nil {
				return nil, err
			}

			if len(temporalBatches) > 0 {
				log.Debug("started txn to apply temporal batches on target")
				tx, err := targetPool.DB.BeginTx(ctx, &sql.TxOptions{})
				if err != nil {
					return nil, errors.Wrapf(err, "failed to begin a txn on the target db")
				}

				log.Debug("start accepting temporal batches")

				for _, tmpBatch := range temporalBatches {
					if err := connAcceptor.AcceptTemporalBatch(ctx, tmpBatch, &types.AcceptOptions{
						TargetQuerier: tx,
					}); err != nil {
						return nil, errors.Wrapf(err, "failed to accept temporal batch")
					}
				}

				log.Debug("finished accepting temporal batches")
				if err := tx.Commit(); err != nil {
					return nil, err
				}
				log.Debug("txn committed")
			}

			log.Debugf("Next SCN:%s", db.SCN)
		case <-ctx.Done():
			return db, nil
		}
	}
}

// logMnrEnabledCheck checks if essential settings required by LogMiner are enabled.
func logMnrEnabledCheck(ctx *stopper.Context, db *sql.DB) error {
	var logModeStr sql.NullString
	if err := db.QueryRowContext(ctx, logModeCheckStmt).Scan(&logModeStr); err != nil {
		return errors.Wrapf(err, "failed to query the log mode")
	}

	if !logModeStr.Valid || logModeStr.String != archiveLog {
		return errors.New("archive log is not enabled")
	}

	var supplementLogStr sql.NullString

	if err := db.QueryRowContext(ctx, supplementalLogDataStmt).Scan(&supplementLogStr); err != nil {
		return errors.Wrapf(err, "failed to check if supplement log has been enabled")
	}

	if !supplementLogStr.Valid || supplementLogStr.String != "YES" {
		return errors.WithMessage(errors.New("supplemental log data is not enabled"), "please run ALTER DATABASE ADD SUPPLEMENTAL LOG DATA")
	}
	log.Info("logMiner is confirmed enabled")

	return nil
}

// ProvideEagerConfig is a hack to move up the evaluation of the user
// script so that the options callbacks can set any non-script-related
// CLI flags. The configuration will be preflighted.
func ProvideEagerConfig(cfg *Config, _ *script.Loader) (*EagerConfig, error) {
	return (*EagerConfig)(cfg), cfg.Preflight()
}
