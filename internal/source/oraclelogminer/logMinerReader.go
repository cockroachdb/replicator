package oraclelogminer

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/source/oraclelogminer/scn"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

var _ types.BatchReader = (*logMinerReader)(nil)

// TODO(janexing): should I directly have DB implement the Read() method?
type logMinerReader struct {
	sourcePool   *types.SourcePool
	initialSCN   scn.SCN
	sourceSchema ident.Schema
	targetSchema ident.Schema
	pullInterval time.Duration
	// Ensure the timestamps we generate always march forward.
	monotonic hlc.Clock
}

func (l *logMinerReader) outputMessage(ctx *stopper.Context, out chan<- *types.BatchCursor) {
	// Create a ticker that triggers every second
	ticker := time.NewTicker(l.pullInterval)
	defer ticker.Stop()

	startSCN := scn.SCN{}

	// Pull based model for acquiring the changefeed logs and convert them into mutations.
	for {
		select {
		case <-ticker.C:
			if startSCN.IsEmpty() {
				startSCN = l.initialSCN
			}
			// TODO(janexing): make the changefeed acquiring executed IN PARALLEL TO the applying logic.
			logs, nextStartSCN, err := readLogsOnce(ctx, l.sourcePool, startSCN, l.sourceSchema.Raw())
			if err != nil {
				out <- &types.BatchCursor{Error: errors.Wrapf(err, "failed to obtain changefeed logs")}
				return
			}
			startSCN = nextStartSCN
			log.Infof("received %d logs", len(logs))
			// Logs with the same XID (transaction ID) will be put into the same temporalBatch.
			// TODO(janexing): set the commit SCN as the ext time for this temporal batch.
			temporalBatch := &types.TemporalBatch{}
			// Once all logs are processed, the batches are sent to conn acceptor one by one.
			temporalBatches := make([]*types.TemporalBatch, 0)

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
					temporalBatches = append(temporalBatches, temporalBatch)
					temporalBatch = &types.TemporalBatch{}
				}

				prevXID = lg.TxnID

				// We need to get the primary key values for the changefeed, as in the update or
				// delete stmt that logminer provides there might not explicitly contains the pk
				// values, but just the rowid.
				if lg.Operation != OperationInsert {
					pkNames, err := GetPKNames(ctx, l.sourcePool, lg.SegOwner, lg.TableName)
					if err != nil {
						out <- &types.BatchCursor{Error: err}
						return
					}
					// TODO(janexing): consider changefeed log where the PK is updated.
					for _, pkName := range pkNames {
						pkVal, ok := kv.WhereKV[pkName]
						if !ok {
							out <- &types.BatchCursor{Error: errors.Errorf("value not found for primary key %s in the changefeed log", pkName)}
							return
						}
						kv.ValKV[pkName] = pkVal
					}
				}

				// Convert the kv struct into a mutation obj.
				byteRes, err := json.Marshal(kv.ValKV)
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

				if temporalBatch.Time.External() == 0 {
					temporalBatch.Time = l.monotonic.External(lg.CommitSCN)
				}
				mut.Time = temporalBatch.Time

				targetTbl := ident.NewTable(l.targetSchema, ident.New(lg.TableName))
				if err := temporalBatch.Accumulate(targetTbl, mut); err != nil {
					out <- &types.BatchCursor{Error: errors.Wrapf(err, "failed to accumulate mut to batch")}
					return
				}
			}

			if temporalBatch.Time.External() != 0 {
				temporalBatches = append(temporalBatches, temporalBatch)
			}

			// TODO(janexing): figure out what else to include in the batch cursor.
			out <- &types.BatchCursor{Batch: temporalBatch}
		case <-ctx.Done():
			return
		}
	}
}

func (l *logMinerReader) Read(ctx *stopper.Context) (<-chan *types.BatchCursor, error) {
	readChan := make(chan *types.BatchCursor, 2)

	ctx.Go(func(ctx *stopper.Context) error {
		l.outputMessage(ctx, readChan)
		return nil
	})
	return readChan, nil
}
