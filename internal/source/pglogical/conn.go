// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package pglogical contains support for reading a PostgreSQL logical
// replication feed.
package pglogical

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	"github.com/google/uuid"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// lsnStamp adapts the LSN offset type to a comparable Stamp value.
type lsnStamp pglogrepl.LSN

var (
	_ stamp.Stamp         = lsnStamp(0)
	_ logical.OffsetStamp = lsnStamp(0)
)

func (s lsnStamp) AsLSN() pglogrepl.LSN        { return pglogrepl.LSN(s) }
func (s lsnStamp) AsOffset() uint64            { return uint64(s) }
func (s lsnStamp) Less(other stamp.Stamp) bool { return s < other.(lsnStamp) }
func (s lsnStamp) MarshalText() (text []byte, err error) {
	return []byte(strconv.FormatInt(int64(s), 10)), nil
}

// A conn encapsulates all wire-connection behavior. It is
// responsible for receiving replication messages and replying with
// status updates.
type conn struct {
	// Columns, as ordered by the source database.
	columns map[ident.Table][]types.ColData
	// The pg publication name to subscribe to.
	publicationName string
	// Map source ids to target tables.
	relations map[uint32]ident.Table
	// The name of the slot within the publication.
	slotName string
	// The configuration for opening replication connections.
	sourceConfig *pgconn.Config
}

var _ logical.Dialect = (*conn)(nil)

// Process implements logical.Dialect and receives a sequence of logical
// replication messages, or possibly a rollbackMessage.
func (c *conn) Process(
	ctx context.Context, ch <-chan logical.Message, events logical.Events,
) error {
	for {
		// Perform context-aware read.
		var msg logical.Message
		select {
		case msg = <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}

		// Ensure that we resynchronize.
		if logical.IsRollback(msg) {
			if err := events.OnRollback(ctx, msg); err != nil {
				return err
			}
		}

		log.Tracef("message %T", msg)
		var err error
		switch msg := msg.(type) {
		case *pglogrepl.RelationMessage:
			// The replication protocol says that we'll see these
			// descriptors before any use of the relation id in the
			// stream. We'll map the int value to our table identifiers.
			c.onRelation(msg, events.GetTargetDB())

		case *pglogrepl.BeginMessage:
			err = events.OnBegin(ctx, lsnStamp(msg.FinalLSN))

		case *pglogrepl.CommitMessage:
			err = events.OnCommit(ctx)

		case *pglogrepl.DeleteMessage:
			err = c.onDataTuple(ctx, events, msg.RelationID, msg.OldTuple, true /* isDelete */)

		case *pglogrepl.InsertMessage:
			err = c.onDataTuple(ctx, events, msg.RelationID, msg.Tuple, false /* isDelete */)

		case *pglogrepl.UpdateMessage:
			err = c.onDataTuple(ctx, events, msg.RelationID, msg.NewTuple, false /* isDelete */)

		case *pglogrepl.TruncateMessage:
			err = errors.Errorf("the TRUNCATE operation cannot be supported on table %d", msg.RelationNum)

		default:
			err = errors.Errorf("unimplemented logical replication message %T", msg)
		}
		if err != nil {
			return err
		}
	}
}

// ReadInto implements logical.Dialect, opens a replication connection,
// and writes parsed messages into the provided channel. This method
// also manages the keepalive protocol.
func (c *conn) ReadInto(ctx context.Context, ch chan<- logical.Message, state logical.State) error {
	replConn, err := pgconn.ConnectConfig(ctx, c.sourceConfig)
	if err != nil {
		return errors.WithStack(err)
	}
	defer replConn.Close(context.Background())

	var startLogPos pglogrepl.LSN
	if x, ok := state.GetConsistentPoint().(lsnStamp); ok {
		startLogPos = x.AsLSN()
	}
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

	standbyTimeout := time.Second * 10
	standbyDeadline := time.Now().Add(standbyTimeout)

	for ctx.Err() == nil {
		if time.Now().After(standbyDeadline) {
			logPos := state.GetConsistentPoint().(lsnStamp).AsLSN()
			err = pglogrepl.SendStandbyStatusUpdate(ctx, replConn, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: logPos,
			})
			if err != nil {
				return errors.WithStack(err)
			}
			log.WithField("WALWritePosition", logPos).Trace("sent Standby status message")
			standbyDeadline = time.Now().Add(standbyTimeout)
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
				select {
				case ch <- logicalMsg:
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
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

// UnmarshalStamp decodes a string representation of a Stamp.
func (c *conn) UnmarshalStamp(stamp []byte) (stamp.Stamp, error) {
	res, err := strconv.ParseInt(string(stamp), 0, 64)
	return lsnStamp(res), err
}

// decodeMutation converts the incoming tuple data into a Mutation.
func (c *conn) decodeMutation(
	tbl ident.Table, data *pglogrepl.TupleData, isDelete bool,
) (types.Mutation, error) {
	var mut types.Mutation
	var key []string
	enc := make(map[string]interface{})
	targetCols, ok := c.columns[tbl]
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
func (c *conn) onDataTuple(
	ctx context.Context,
	events logical.Events,
	relation uint32,
	tuple *pglogrepl.TupleData,
	isDelete bool,
) error {
	traceTuple(tuple)
	tbl, ok := c.relations[relation]
	if !ok {
		return errors.Errorf("unknown relation id %d", relation)
	}
	mut, err := c.decodeMutation(tbl, tuple, isDelete)
	if err != nil {
		return err
	}

	return events.OnData(ctx, tbl, []types.Mutation{mut})
}

// learn updates the source database namespace mappings.
func (c *conn) onRelation(msg *pglogrepl.RelationMessage, targetDB ident.Ident) {
	// The replication protocol says that we'll see these
	// descriptors before any use of the relation id in the
	// stream. We'll map the int value to our table identifiers.
	tbl := ident.NewTable(
		targetDB,
		ident.New(msg.Namespace),
		ident.New(msg.RelationName))
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
	c.columns[tbl] = colNames

	log.WithFields(log.Fields{
		"Columns":    colNames,
		"RelationID": msg.RelationID,
		"Table":      tbl,
	}).Trace("learned relation")
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
