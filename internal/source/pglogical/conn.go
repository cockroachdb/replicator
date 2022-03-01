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
	"sync"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/source/cdc"
	"github.com/cockroachdb/cdc-sink/internal/target/apply"
	"github.com/cockroachdb/cdc-sink/internal/target/schemawatch"
	"github.com/cockroachdb/cdc-sink/internal/target/stage"
	"github.com/cockroachdb/cdc-sink/internal/target/timekeeper"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/batches"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/google/uuid"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// A Conn encapsulates all wire-connection behavior. It is
// responsible for receiving replication messages and replying with
// status updates.
type Conn struct {
	// Apply mutations to the backing store.
	appliers types.Appliers
	// Columns, as ordered by the source database.
	columns map[ident.Table][]types.ColData
	// Tables that need to be drained.
	dirty     map[ident.Table]struct{}
	immediate bool
	// Batches of mutations to stage.
	pending map[ident.Table][]types.Mutation
	// Callbacks to release the slices in pending.
	pendingReleases []func()
	// The pg publication name to subscribe to.
	publicationName string
	// Map source ids to target tables.
	relations map[uint32]ident.Table
	// Stage mutations in the backing store.
	stagers types.Stagers
	// The name of the slot within the publication.
	slotName string
	// The configuration for opening replication connections.
	sourceConfig *pgconn.Config
	// The SQL database we're going to be writing into.
	targetDB ident.Ident
	// Connection string for the target database.
	targetPool *pgxpool.Pool
	// Likely nil.
	testControls *TestControls
	// Drain.
	timeKeeper types.TimeKeeper
	// The (eventual) commit time of the transaction being processed.
	txTime hlc.Time

	mu struct {
		sync.Mutex

		// This is the position in the transaction log that we'll
		// occasionally report back to the server. It is updated when we
		// successfully commit an entire transaction's worth of data.
		clientXLogPos pglogrepl.LSN
	}
}

// NewConn constructs a new pglogical replication feed.
//
// The feed will terminate when the context is canceled and the stopped
// channel will be closed once shutdown is complete.
func NewConn(ctx context.Context, config *Config) (_ *Conn, stopped <-chan struct{}, _ error) {
	// Some pre-flight checks.
	if config.Publication == "" {
		return nil, nil, errors.New("no publication name was configured")
	}
	if config.Slot == "" {
		return nil, nil, errors.New("no replication slot name was configured")
	}
	if config.SourceConn == "" {
		return nil, nil, errors.New("no source connection was configured")
	}
	if config.TargetConn == "" {
		return nil, nil, errors.New("no target connection was configured")
	}
	if config.TargetDB.IsEmpty() {
		return nil, nil, errors.New("no target db was configured")
	}

	// Verify that the publication and replication slots were configured
	// by the user. We could create the replication slot ourselves, but
	// we want to coordinate the timing of the backup, restore, and
	// streaming operations.
	source, err := pgx.Connect(ctx, config.SourceConn)
	if err != nil {
		return nil, nil, errors.Wrap(err, "could not connect to source database")
	}
	defer source.Close(context.Background())

	// Ensure that the requested publication exists.
	var count int
	if err := source.QueryRow(ctx,
		"SELECT count(*) FROM pg_publication WHERE pubname = $1",
		config.Publication,
	).Scan(&count); err != nil {
		return nil, nil, errors.WithStack(err)
	}
	if count != 1 {
		return nil, nil, errors.Errorf(
			`run CREATE PUBLICATION %s FOR ALL TABLES; in source database`,
			config.Publication)
	}
	log.Tracef("validated that publication %q exists", config.Publication)

	// Verify that the consumer slot exists.
	if err := source.QueryRow(ctx,
		"SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1",
		config.Slot,
	).Scan(&count); err != nil {
		return nil, nil, errors.WithStack(err)
	}
	if count != 1 {
		return nil, nil, errors.Errorf(
			"run SELECT pg_create_logical_replication_slot('%s', 'pgoutput'); in source database, "+
				"then perform bulk data copy",
			config.Slot)
	}
	log.Tracef("validated that replication slot %q exists", config.Slot)

	// Copy the configuration and tweak it for replication behavior.
	sourceConfig := source.Config().Config.Copy()
	sourceConfig.RuntimeParams["replication"] = "database"

	// Bring up connection to target database.
	targetCfg, err := pgxpool.ParseConfig(config.TargetConn)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "could not parse %q", config.TargetConn)
	}
	// Identify traffic.
	targetCfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "SET application_name=$1", "cdc-sink")
		return err
	}
	// Ensure connection diversity through long-lived loadbalancers.
	targetCfg.MaxConnLifetime = 10 * time.Minute
	// Keep one spare connection.
	targetCfg.MinConns = 1

	targetPool, err := pgxpool.ConnectConfig(ctx, targetCfg)
	if err != nil {
		return nil, nil, errors.Wrap(err, "could not connect to CockroachDB")
	}

	timeKeeper, err := timekeeper.NewTimeKeeper(ctx, targetPool, cdc.Resolved)
	if err != nil {
		return nil, nil, err
	}

	watchers, cancelWatchers := schemawatch.NewWatchers(targetPool)
	appliers, cancelAppliers := apply.NewAppliers(watchers)
	stagers := stage.NewStagers(targetPool, ident.StagingDB)

	ret := &Conn{
		appliers:        appliers,
		columns:         make(map[ident.Table][]types.ColData),
		dirty:           make(map[ident.Table]struct{}),
		immediate:       config.Immediate,
		pending:         make(map[ident.Table][]types.Mutation),
		publicationName: config.Publication,
		relations:       make(map[uint32]ident.Table),
		slotName:        config.Slot,
		sourceConfig:    sourceConfig,
		stagers:         stagers,
		targetDB:        config.TargetDB,
		targetPool:      targetPool,
		testControls:    config.TestControls,
		timeKeeper:      timeKeeper,
	}

	stopper := make(chan struct{})
	go func() {
		_ = ret.run(ctx)
		cancelAppliers()
		cancelWatchers()
		targetPool.Close()
		close(stopper)
	}()

	return ret, stopper, nil
}

// run blocks while the connection is processing messages.
func (c *Conn) run(ctx context.Context) error {
	for ctx.Err() == nil {
		group, ctx := errgroup.WithContext(ctx)

		// Start a background goroutine to maintain the replication
		// connection. This source goroutine is set up to be robust; if
		// there's an error talking to the source database, we send a
		// rollback message to the consumer and retry the connection.
		ch := make(chan pglogrepl.Message, 16)
		group.Go(func() error {
			defer close(ch)
			for ctx.Err() == nil {
				if err := c.readReplicationData(ctx, ch); err != nil {
					log.WithError(err).Error("error from replication channel; continuing")
				}
				select {
				case ch <- &rollbackMessage{}:
				case <-ctx.Done():
					return nil
				}
			}
			return nil
		})

		// This goroutine applies the incoming mutations to the target
		// database. It is fragile, when it errors, we need to also
		// restart the source goroutine.
		group.Go(func() error {
			err := c.processMessages(ctx, ch)
			if err != nil {
				log.WithError(err).Error("error while applying replication messages; stopping")
			}
			return err
		})

		if err := group.Wait(); err != nil {
			log.WithError(err).Error("error in replication loop; restarting")
		}
	}
	log.Info("shut down replication loop")
	return nil
}

func (c *Conn) getLogPos() pglogrepl.LSN {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mu.clientXLogPos
}

func (c *Conn) setLogPos(pos pglogrepl.LSN) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.clientXLogPos = pos
	log.WithField("LSN", pos).Trace("updated LSN")
	lsnOffset.Set(float64(pos))
}

// decodeMutation converts the incoming tuple data into a Mutation.
func (c *Conn) decodeMutation(
	tbl ident.Table, data *pglogrepl.TupleData, isDelete bool,
) (types.Mutation, error) {
	mut := types.Mutation{Time: c.txTime}
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

// flush commits all pending mutations to their respective stages or
// applies them in immediate-mode. It will also zero-out the length of
// the associated pending slice.
func (c *Conn) flush(ctx context.Context, tbl ident.Table) error {
	if ctrl := c.testControls; ctrl != nil {
		if fn := c.testControls.BreakSinkFlush; fn != nil {
			if fn() {
				return errors.New("breaking sink flush for test")
			}
		}
	}

	found := c.pending[tbl]
	if len(found) == 0 {
		return nil
	}

	if c.immediate {
		// TODO(bob): Eventually we'll have data-driven configuration.
		app, err := c.appliers.Get(ctx, tbl, nil /* cas */, types.Deadlines{})
		if err != nil {
			return err
		}
		if err := app.Apply(ctx, c.targetPool, found); err != nil {
			return err
		}
	} else {
		stager, err := c.stagers.Get(ctx, tbl)
		if err != nil {
			return err
		}
		if err := stager.Store(ctx, c.targetPool, found); err != nil {
			return err
		}
	}
	c.pending[tbl] = found[:0]
	return nil
}

// learn updates the source database namespace mappings.
func (c *Conn) learn(msg *pglogrepl.RelationMessage) {
	// The replication protocol says that we'll see these
	// descriptors before any use of the relation id in the
	// stream. We'll map the int value to our table identifiers.
	tbl := ident.NewTable(
		c.targetDB,
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

// onCommit ensure that all in-memory data has been committed to the
// target cluster. It will also update the WAL position that we report
// back to the source database.
func (c *Conn) onCommit(ctx context.Context, msg *pglogrepl.CommitMessage) error {
	// Flush in-memory data to the staging tables.
	for tbl := range c.pending {
		if err := c.flush(ctx, tbl); err != nil {
			return err
		}
	}

	// In immediate mode, the call(s) to flush() above will have
	// committed to the target tables. Otherwise, apply the transaction
	// data, through the usual drain-and-apply mechanism.
	if !c.immediate {
		if err := retry.Retry(ctx, func(ctx context.Context) error {
			tx, err := c.targetPool.Begin(ctx)
			if err != nil {
				return errors.WithStack(err)
			}
			defer tx.Rollback(ctx)

			for tbl := range c.dirty {
				prev, err := c.timeKeeper.Put(ctx, tx, tbl.AsSchema(), c.txTime)
				if err != nil {
					return err
				}

				stage, err := c.stagers.Get(ctx, tbl)
				if err != nil {
					return err
				}

				muts, err := stage.Drain(ctx, tx, prev, c.txTime)
				if err != nil {
					return err
				}

				app, err := c.appliers.Get(ctx, tbl, nil /* casColumns */, types.Deadlines{})
				if err != nil {
					return err
				}

				if err := app.Apply(ctx, tx, muts); err != nil {
					return err
				}
			}
			return tx.Commit(ctx)
		}); err != nil {
			return err
		}
	}

	// Advance our high-water mark within the source's WAL,
	// which will be reported by the keepalive loop.
	c.setLogPos(msg.TransactionEndLSN)
	c.reset()
	commitCount.Inc()
	commitTime.Set(float64(msg.CommitTime.Unix()))
	return nil
}

// onDataTuple will add an incoming row tuple to the in-memory slice,
// possibly flushing it when the batch size limit is reached.
func (c *Conn) onDataTuple(
	ctx context.Context, relation uint32, tuple *pglogrepl.TupleData, isDelete bool,
) error {
	if ctrl := c.testControls; ctrl != nil {
		if fn := c.testControls.BreakOnDataTuple; fn != nil {
			if fn() {
				return errors.New("breaking onDataTuple for test")
			}
		}
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

	found := c.pending[tbl]
	if found == nil {
		next, fn := batches.Mutation()
		c.pendingReleases = append(c.pendingReleases, fn)
		found = next
	}
	found = append(found, mut)
	c.dirty[tbl] = struct{}{}
	c.pending[tbl] = found
	if len(found) == cap(found) {
		return c.flush(ctx, tbl)
	}
	return nil
}

// processMessages receives a sequence of logical replication messages,
// or possibly a rollbackMessage. If this loop fails out, we'll need to
// stop and restart the underlying replication network connection.
func (c *Conn) processMessages(ctx context.Context, ch <-chan pglogrepl.Message) error {
	for msg := range ch {
		log.Tracef("message %T", msg)
		var err error
		switch msg := msg.(type) {
		case *rollbackMessage:
			// This is a custom message that we'll insert to indicate
			// that the upstream message provider is going to restart
			// the feed.
			c.reset()

		case *pglogrepl.RelationMessage:
			// The replication protocol says that we'll see these
			// descriptors before any use of the relation id in the
			// stream. We'll map the int value to our table identifiers.
			c.learn(msg)

		case *pglogrepl.BeginMessage:
			// Starting a new transaction. We're going to accept the
			// transaction time as reported by the server as our
			// HLC timestamp for staging purposes.
			c.txTime = hlc.From(msg.CommitTime)

		case *pglogrepl.CommitMessage:
			err = c.onCommit(ctx, msg)

		case *pglogrepl.DeleteMessage:
			err = c.onDataTuple(ctx, msg.RelationID, msg.OldTuple, true /* isDelete */)

		case *pglogrepl.InsertMessage:
			err = c.onDataTuple(ctx, msg.RelationID, msg.Tuple, false /* isDelete */)

		case *pglogrepl.UpdateMessage:
			err = c.onDataTuple(ctx, msg.RelationID, msg.NewTuple, false /* isDelete */)

		case *pglogrepl.TruncateMessage:
			err = errors.Errorf("the TRUNCATE operation cannot be supported on table %d", msg.RelationNum)

		default:
			err = errors.Errorf("unimplemented logical replication message %T", msg)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// readReplicationData opens a replication connection and writes parsed
// messages into the provided channel. This method also manages the
// keepalive protocol. The consuming code should call Conn.setLogPos
// in order to advance the server-side high-water mark.
func (c *Conn) readReplicationData(ctx context.Context, ch chan<- pglogrepl.Message) error {
	replConn, err := pgconn.ConnectConfig(ctx, c.sourceConfig)
	if err != nil {
		return errors.WithStack(err)
	}
	defer replConn.Close(context.Background())

	if err := pglogrepl.StartReplication(ctx,
		replConn, c.slotName, c.getLogPos(),
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
		if ctrl := c.testControls; ctrl != nil {
			if fn := c.testControls.BreakReplicationFeed; fn != nil {
				if fn() {
					log.Debug("closing replication connection for test")
					_ = replConn.Close(ctx)
				}
			}
		}

		if time.Now().After(standbyDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(ctx, replConn, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: c.getLogPos(),
			})
			if err != nil {
				return errors.WithStack(err)
			}
			log.Trace("Sent Standby status message")
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

// reset abandons any in-flight data that we've accumulated.
func (c *Conn) reset() {
	c.dirty = make(map[ident.Table]struct{})
	for tbl, muts := range c.pending {
		c.pending[tbl] = muts[:0]
	}
	c.txTime = hlc.Zero()
}

// traceTuple
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

// A rollbackMessage is inserted into the message channel to indicate
// that some error occurred and the receiver should unwind to a safe
// state.
type rollbackMessage struct{}

var _ pglogrepl.Message = &rollbackMessage{}

// Type implements pglogrepl.Message and returns a value which is
// not used by any real message type.
func (r rollbackMessage) Type() pglogrepl.MessageType {
	return '!'
}
