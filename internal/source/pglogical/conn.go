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

// Package pglogical contains support for reading a PostgreSQL logical
// replication feed.
package pglogical

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/notify"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/cockroachdb/cdc-sink/internal/util/stopvar"
	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// A Conn encapsulates all wire-connection behavior. It is
// responsible for receiving replication messages and replying with
// status updates.
type Conn struct {
	// The destination for writes.
	acceptor types.MultiAcceptor
	// Columns, as ordered by the source database.
	columns *ident.TableMap[[]types.ColData]
	// The time of the transaction being received.
	nextCommitTime time.Time
	// Persistent storage for WAL data.
	memo types.Memo
	// The pg publication name to subscribe to.
	publicationName string
	// Map source ids to target tables.
	relations map[uint32]ident.Table
	// The name of the slot within the publication.
	slotName string
	// The configuration for opening replication connections.
	sourceConfig *pgconn.Config
	// How ofter to commit the consistent point
	standbyTimeout time.Duration
	// Access to the staging cluster.
	stagingDB *types.StagingPool
	// The destination for writes.
	target ident.Schema
	// Access to the target database.
	targetDB *types.TargetPool
	// Support for toasted columns
	toastedColumns bool
	// Managed by persistWALOffset.
	walOffset notify.Var[pglogrepl.LSN]
}

// Start launches goroutines into the context.
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
				case <-time.After(100 * time.Millisecond):
				}
			}
		}
		return nil
	})

	return nil
}

// accumulateBatch folds replication messages into the batch and sends it to
// the acceptor when a complete transaction has been read. The returned
// batch should be passed to the next invocation of accumulateBatch.
func (c *Conn) accumulateBatch(
	ctx *stopper.Context, msg pglogrepl.Message, batch *types.MultiBatch,
) (*types.MultiBatch, error) {
	log.Tracef("message %T", msg)
	switch msg := msg.(type) {
	case *pglogrepl.RelationMessage:
		// The replication protocol says that we'll see these
		// descriptors before any use of the relation id in the
		// stream. We'll map the int value to our table identifiers.
		c.onRelation(msg)
		return batch, nil

	case *pglogrepl.BeginMessage:
		c.nextCommitTime = msg.CommitTime
		// Create a new batch to accumulate into. It may be discarded
		// later if the timestamp precedes the latest commit.
		return &types.MultiBatch{}, nil

	case *pglogrepl.CommitMessage:
		// We rely on the upstream database to replay events in the case of
		// errors, so we may receive events that we've already processed.
		// We use the COMMIT LSN offset as the consistent point, which is
		// written in the WAL synchronously with the upstream transaction.
		ignoreLSN, _ := c.walOffset.Get()

		if msg.CommitLSN <= ignoreLSN {
			// Just discard the entire batch in this case.
			log.Tracef("ignoring CommitMessage at %s before %s",
				msg.CommitLSN, ignoreLSN)
			return nil, nil
		}
		// In Postgres version < v15, the stream might contain empty transactions.
		// See https://github.com/postgres/postgres/commit/d5a9d86d8f
		// We will skip them to avoid unnecessary writes to the memo table.
		if batch.Count() == 0 {
			emptyTransactionCount.Inc()
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
		c.walOffset.Set(msg.CommitLSN)
		return nil, nil

	case *pglogrepl.DeleteMessage:
		return batch, c.onDataTuple(batch, msg.RelationID, msg.OldTuple, true /* isDelete */)

	case *pglogrepl.InsertMessage:
		return batch, c.onDataTuple(batch, msg.RelationID, msg.Tuple, false /* isDelete */)

	case *pglogrepl.UpdateMessage:
		return batch, c.onDataTuple(batch, msg.RelationID, msg.NewTuple, false /* isDelete */)

	case *pglogrepl.TruncateMessage:
		return nil, errors.Errorf("the TRUNCATE operation cannot be supported on table %d", msg.RelationNum)

	case *pglogrepl.TypeMessage:
		// This type is intentionally discarded. We interpret the
		// type of the data based on the target table, not the
		// source.
		return batch, nil

	default:
		return nil, errors.Errorf("unimplemented logical replication message %T", msg)
	}
}

// copyMessages is the main replication loop. It will open a connection
// to the source, accumulate messages, and commit data to the target.
func (c *Conn) copyMessages(ctx *stopper.Context) error {
	replConn, err := pgconn.ConnectConfig(ctx, c.sourceConfig)
	if err != nil {
		return errors.WithStack(err)
	}
	defer replConn.Close(context.Background())

	startLogPos, _ := c.walOffset.Get()
	if err := pglogrepl.StartReplication(ctx,
		replConn, c.slotName, startLogPos,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '1'",
				fmt.Sprintf("publication_names '%s'", c.publicationName)},
		},
	); err != nil {
		dialFailureCount.Inc()
		return errors.WithStack(err)
	}
	dialSuccessCount.Inc()

	var batch *types.MultiBatch
	standbyDeadline := time.Now().Add(c.standbyTimeout)

	for !ctx.IsStopping() {
		// Occasionally send updates back to the server so it will
		// remember our WAL offset.
		if time.Now().After(standbyDeadline) {
			standbyDeadline = time.Now().Add(c.standbyTimeout)
			lsn, _ := c.walOffset.Get()
			if err := pglogrepl.SendStandbyStatusUpdate(ctx, replConn,
				pglogrepl.StandbyStatusUpdate{
					WALWritePosition: lsn,
				},
			); err != nil {
				return errors.WithStack(err)
			}
			log.WithField("WALWritePosition", lsn).Trace("sent Standby status message")
		}

		// Receive one message, with a timeout. In a low-traffic
		// situation, we want to ensure that we're sending heartbeats
		// back to the source server.
		receiveCtx, cancel := context.WithDeadline(ctx, standbyDeadline)
		msg, err := replConn.ReceiveMessage(receiveCtx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return errors.WithStack(err)
		}
		log.Tracef("received %T", msg)

		switch msg := msg.(type) {
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				// The server is sending us a keepalive message. This is
				// informational, except in the case where an immediate
				// acknowledgement is requested.  In that case, we'll
				// reset the standby deadline to zero, so we kick back a
				// message at the top of the loop.
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					return errors.WithStack(err)
				}
				log.WithFields(log.Fields{
					"ServerWALEnd":   pkm.ServerWALEnd,
					"ServerTime":     pkm.ServerTime,
					"ReplyRequested": pkm.ReplyRequested,
				}).Debug("primary keepalive received")

				if pkm.ReplyRequested {
					standbyDeadline = time.Time{}
				}

			case pglogrepl.XLogDataByteID:
				// This is where things get interesting. We have actual
				// transaction log data to parse into messages. These
				// messages get handed off to the consumer via the
				// channel passed in.
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					return errors.WithStack(err)
				}
				log.WithFields(log.Fields{
					"ByteCount":    len(xld.WALData),
					"ServerWALEnd": xld.ServerWALEnd,
					"ServerTime":   xld.ServerTime,
					"WALStart":     xld.WALStart,
				}).Debug("xlog data")

				logicalMsg, err := pglogrepl.Parse(xld.WALData)
				if err != nil {
					return errors.WithStack(err)
				}
				log.WithFields(log.Fields{
					"logicalMsg": logicalMsg.Type().String(),
				}).Debug("xlog data")

				// Update our accumulator with the received message.
				batch, err = c.accumulateBatch(ctx, logicalMsg, batch)
				if err != nil {
					return err
				}
			}
		case *pgproto3.NotificationResponse:
			log.Debugf("notification from server: %s", msg.Payload)
		default:
			log.Debugf("unexpected payload message: %T", msg)
		}
	}
	return nil
}

// decodeMutation converts the incoming tuple data into a Mutation.
func (c *Conn) decodeMutation(
	tbl ident.Table, data *pglogrepl.TupleData, isDelete bool,
) (types.Mutation, error) {
	var mut types.Mutation
	var key []string
	enc := make(map[string]any)
	targetCols, ok := c.columns.Get(tbl)
	if !ok {
		return mut, errors.Errorf("no column data for %s", tbl)
	}
	if len(targetCols) != len(data.Columns) {
		return mut, errors.Errorf("column count mismatch is %s: %d vs %d",
			tbl, len(targetCols), len(data.Columns))
	}
	for idx, sourceCol := range data.Columns {
		targetCol := targetCols[idx]
		switch sourceCol.DataType {
		case pglogrepl.TupleDataTypeNull:
			enc[targetCol.Name.Raw()] = nil
		case pglogrepl.TupleDataTypeText:
			// The incoming data is in a textual format.
			enc[targetCol.Name.Raw()] = string(sourceCol.Data)
			if targetCol.Primary {
				key = append(key, string(sourceCol.Data))
			}
		case pglogrepl.TupleDataTypeToast:
			if c.toastedColumns {
				// TupleDataTypeToast is just a marker that tells us
				// that a TOASTed column has not changed.
				// Putting a placeholders for downstream apply handlers,
				// and a custom template that will be able to handle it.
				unchangedToastedColumns.Inc()
				if mut.Meta == nil {
					mut.Meta = make(map[string]any)
				}
				mut.Meta[types.CustomUpsert] = "toasted"
				enc[targetCol.Name.Raw()] = types.ToastedColumnPlaceholder
				continue
			}
			return mut, errors.Errorf(
				"TOASTed columns are not supported in %s.%s", tbl, targetCol.Name)
		default:
			return mut, errors.Errorf(
				"unimplemented tuple data type %q", string(sourceCol.DataType))
		}
	}

	// In the pathological case where a table has no primary key, we'll
	// generate a random uuid value to use as the staging key. This is
	// fine, because the underlying data has no particular identity to
	// update. In fact, it's not possible to issue an UPDATE to Postgres
	// when a row has no replication identity.
	if len(key) == 0 {
		key = []string{uuid.New().String()}
	}
	var err error
	mut.Key, err = json.Marshal(key)
	if err != nil {
		return mut, errors.WithStack(err)
	}
	// We don't need the actual column data for delete operations.
	if !isDelete {
		mut.Data, err = json.Marshal(enc)
		if err != nil {
			return mut, errors.WithStack(err)
		}
	}
	return mut, errors.WithStack(err)
}

// onDataTuple will add an incoming row tuple to the in-memory slice,
// possibly flushing it when the batch size limit is reached.
func (c *Conn) onDataTuple(
	batch *types.MultiBatch, relation uint32, tuple *pglogrepl.TupleData, isDelete bool,
) error {
	if batch == nil {
		log.Trace("ignoring replayed message")
		return nil
	}
	traceTuple(tuple)
	tbl, ok := c.relations[relation]
	if !ok {
		return errors.Errorf("unknown relation id %d", relation)
	}
	mut, err := c.decodeMutation(tbl, tuple, isDelete)
	if err != nil {
		return err
	}
	// Set an approximate timestamp.
	mut.Time = hlc.New(c.nextCommitTime.UnixNano(), 0)
	// Set script metadata, which will be acted on by the acceptor.
	script.AddMeta("pglogical", tbl, &mut)

	return batch.Accumulate(tbl, mut)
}

// learn updates the source database namespace mappings.
func (c *Conn) onRelation(msg *pglogrepl.RelationMessage) {
	// The replication protocol says that we'll see these
	// descriptors before any use of the relation id in the
	// stream. We'll map the int value to our table identifiers.
	tbl := ident.NewTable(c.target, ident.New(msg.RelationName))
	c.relations[msg.RelationID] = tbl

	colNames := make([]types.ColData, len(msg.Columns))
	for idx, col := range msg.Columns {
		colNames[idx] = types.ColData{
			Name:    ident.New(col.Name),
			Primary: col.Flags == 1,
			// This could be made textual if we used the
			// ConnInfo metadata methods.
			Type: fmt.Sprintf("%d", col.DataType),
		}
	}
	c.columns.Put(tbl, colNames)

	log.WithFields(log.Fields{
		"Columns":    colNames,
		"RelationID": msg.RelationID,
		"Table":      tbl,
	}).Trace("learned relation")
}

// persistWALOffset loads an existing value from memo into walOffset. It
// will also start a goroutine in the stopper to occasionally write an
// updated value back to the memo.
func (c *Conn) persistWALOffset(ctx *stopper.Context) error {
	key := fmt.Sprintf("pglogical-wal-offset-%s", c.target.Raw())
	found, err := c.memo.Get(ctx, c.stagingDB, key)
	if err != nil {
		return err
	}
	var lsn pglogrepl.LSN
	if len(found) > 0 {
		if err := lsn.Scan(found); err != nil {
			return errors.WithStack(err)
		}
		c.walOffset.Set(lsn)
	}
	ctx.Go(func() error {
		_, err := stopvar.DoWhenChanged(ctx, lsn, &c.walOffset,
			func(ctx *stopper.Context, _, lsn pglogrepl.LSN) error {
				if err := c.memo.Put(ctx, c.stagingDB, key, []byte(lsn.String())); err == nil {
					log.Tracef("stored WAL offset %s: %s", key, lsn)
				} else {
					log.WithError(err).Warn("could not persist LSN offset")
				}
				return nil
			})
		return err
	})
	return nil
}

// traceTuple emits log messages if tracing is enabled.
func traceTuple(t *pglogrepl.TupleData) {
	if !log.IsLevelEnabled(log.TraceLevel) {
		return
	}
	if t == nil {
		log.Trace("NIL TUPLE")
		return
	}
	s := make([]string, len(t.Columns))
	for idx, data := range t.Columns {
		if data.DataType == pglogrepl.TupleDataTypeText {
			s[idx] = string(data.Data)
		}
	}
	log.WithField("data", s).Trace("values")
}
