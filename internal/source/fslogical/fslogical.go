// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package fslogical contains a logical-replication loop for streaming
// document collections from Google Cloud Firestore.
package fslogical

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Dialect reads data from Google Cloud Firestore.
type Dialect struct {
	backfillDone bool
	coll         *firestore.CollectionRef
	cfg          *loopConfig
	fs           *firestore.Client
}

var (
	_ logical.Backfiller = (*Dialect)(nil)
	_ logical.Dialect    = (*Dialect)(nil)
)

// These are the Dialect message types.
type (
	batchStart  struct{ cp consistentPoint }
	batchDelete struct{ ref *firestore.DocumentRef }
	batchDoc    struct{ doc *firestore.DocumentSnapshot }
	batchEnd    struct{}
)

// BackfillInto implements logical.Dialect. It uses an ID-based cursor
// approach to scan documents in their updated-at order.
func (d *Dialect) BackfillInto(
	ctx context.Context, ch chan<- logical.Message, state logical.State,
) error {
	// From will either be the update time of the last streamed record,
	// or the range that we were backfilling from before being
	// interrupted. If we were in the middle of backfilling, then we
	// also want to pick up from the last document that was processed.
	cp, _ := state.GetConsistentPoint().(consistentPoint)

	var moreWork bool
	var err error
	to := time.Now()

	for {
		cp, moreWork, err = d.backfillOneBatch(ctx, ch, to, cp)
		if err != nil {
			return err
		}
		if moreWork {
			continue
		}
		// If we've spent a non-trivial amount of time to complete
		// the backfill, we may want to start a second backfill
		// to catch up closer to a streaming point.
		if time.Since(to) >= time.Minute {
			to = time.Now()
			continue
		}
		d.backfillDone = true
		log.Infof("finished backfilling collection %s", d.cfg.SourceCollection)
		return nil
	}
}

// backfillOneBatch grabs a single batch of documents from the backend.
// It will return the next incremental consistentPoint and whether the
// backfill is expected to continue.
func (d *Dialect) backfillOneBatch(
	ctx context.Context, ch chan<- logical.Message, now time.Time, cp consistentPoint,
) (_ consistentPoint, moreWork bool, _ error) {

	// Iterate over the collection by (updated_at, __doc_id__) using
	// a cursor-like approach so that we can checkpoint along the way.
	q := d.coll.
		OrderBy(d.cfg.UpdatedAtProperty.Raw(), firestore.Asc).
		OrderBy(firestore.DocumentID, firestore.Asc).
		Where(d.cfg.UpdatedAtProperty.Raw(), "<=", now).
		Limit(d.cfg.BackfillBatch)
	if !cp.IsZero() {
		if cp.AsID() == "" {
			q = q.Where(d.cfg.UpdatedAtProperty.Raw(), ">=", cp.AsTime())
		} else {
			q = q.StartAfter(cp.AsTime(), cp.AsID())
		}
	}
	// We're going to call GetAll since we're running with a reasonable
	// limit value.  This allows us to peek at the id of the last
	// document, so we can compute the eventual consistent point for
	// this batch of docs.
	docs, err := q.Documents(ctx).GetAll()
	if err != nil {
		return cp, false, errors.WithStack(err)
	}
	if len(docs) == 0 {
		return cp, false, nil
	}
	lastDoc := docs[len(docs)-1]
	lastReadID := lastDoc.Ref.ID
	lastReadTime, err := d.docUpdatedAt(lastDoc)
	if err != nil {
		return cp, false, err
	}
	cp = backfillPoint(lastReadID, lastReadTime)

	select {
	case ch <- batchStart{cp}:
	case <-ctx.Done():
		return cp, false, ctx.Err()
	}

	for _, doc := range docs {
		select {
		case ch <- batchDoc{doc}:
		case <-ctx.Done():
			return cp, false, ctx.Err()
		}
	}

	select {
	case ch <- batchEnd{}:
	case <-ctx.Done():
		return cp, false, ctx.Err()
	}

	return cp, len(docs) == d.cfg.BackfillBatch, nil
}

// ShouldBackfill returns true if the backfill process has caught up
// to within one minute of the current time.
func (d *Dialect) ShouldBackfill(logical.State) bool {
	return !d.backfillDone
}

// ReadInto implements logical.Dialect and subscribes to streaming
// updates from the source.
func (d *Dialect) ReadInto(
	ctx context.Context, ch chan<- logical.Message, state logical.State,
) error {
	cp, _ := state.GetConsistentPoint().(consistentPoint)
	// Stream from the last updated time.
	q := d.coll.
		OrderBy(d.cfg.UpdatedAtProperty.Raw(), firestore.Asc).
		StartAt(cp.AsTime())
	snaps := q.Snapshots(ctx)

	for {
		snap, err := snaps.Next()
		if err != nil {
			return errors.WithStack(err)
		}

		select {
		case ch <- batchStart{streamPoint(snap.ReadTime)}:
		case <-ctx.Done():
			return ctx.Err()
		}

		for _, change := range snap.Changes {
			switch change.Kind {
			case firestore.DocumentAdded,
				firestore.DocumentModified:
				select {
				case ch <- batchDoc{change.Doc}:
				case <-ctx.Done():
					return ctx.Err()
				}
			case firestore.DocumentRemoved:
				select {
				case ch <- batchDelete{change.Doc.Ref}:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}

		select {
		case ch <- batchEnd{}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Process implements logical.Dialect.
func (d *Dialect) Process(
	ctx context.Context, ch <-chan logical.Message, events logical.Events,
) error {
	for msg := range ch {
		if logical.IsRollback(msg) {
			if err := events.OnRollback(ctx, msg); err != nil {
				return err
			}
			continue
		}

		switch t := msg.(type) {
		case batchStart:
			if err := events.OnBegin(ctx, t.cp); err != nil {
				return err
			}

		case batchDoc:
			doc := t.doc
			docUpdatedAt, err := d.docUpdatedAt(doc)
			if err != nil {
				return err
			}

			mut, err := d.marshalMutation(doc, docUpdatedAt)
			if err != nil {
				return err
			}

			if err := events.OnData(ctx, d.cfg.TargetTable, []types.Mutation{mut}); err != nil {
				return err
			}

		case batchEnd:
			if err := events.OnCommit(ctx); err != nil {
				return err
			}

		default:
			panic(fmt.Sprintf("unimplemented type %T", msg))
		}
	}
	return nil
}

// UnmarshalStamp implements logical.Dialect. It delegates to
// time.Time.UnmarshalText.
func (d *Dialect) UnmarshalStamp(bytes []byte) (stamp.Stamp, error) {
	var ts consistentPoint
	err := ts.UnmarshalText(bytes)
	return ts, err
}

// docUpdatedAt extracts a timestamp from the document.
func (d *Dialect) docUpdatedAt(doc *firestore.DocumentSnapshot) (time.Time, error) {
	val, err := doc.DataAt(d.cfg.UpdatedAtProperty.Raw())
	if err != nil {
		return time.Time{}, errors.WithStack(err)
	}
	if t, ok := val.(time.Time); ok {
		return t, nil
	}
	return time.Time{}, errors.Errorf("document missing %q property", d.cfg.UpdatedAtProperty.Raw())
}

func (d *Dialect) marshalMutation(
	doc *firestore.DocumentSnapshot, updatedAt time.Time,
) (types.Mutation, error) {
	dataMap := doc.Data()
	// Allow the doc id to be baked into the mutation.
	if d.cfg.DocIDProperty != "" {
		dataMap[d.cfg.DocIDProperty] = doc.Ref.ID
	}
	data, err := json.Marshal(dataMap)
	if err != nil {
		return types.Mutation{}, errors.WithStack(err)
	}

	key, err := json.Marshal([]string{doc.Ref.ID})
	if err != nil {
		return types.Mutation{}, errors.WithStack(err)
	}

	return types.Mutation{
		Data: data,
		Key:  key,
		Time: hlc.New(updatedAt.UnixNano(), 0),
	}, nil
}
