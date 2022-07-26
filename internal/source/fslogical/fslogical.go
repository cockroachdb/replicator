// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package fslogical

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
)

type consistentPoint struct {
	// A timestamp that we have performed a backfill to. This value
	// will only be present when a backfill is underway.
	BackfillTime *time.Time `json:"b,omitempty"`
	// The last document ID read when backfilling.
	BackfillID *string `json:"i,omitempty"`
	// A server-generated timestamp; only updated when a backfill has
	// completed or when we receive new data in streaming mode.
	StreamTime *time.Time `json:"t,omitempty"`
}

// AsTime implements the optional logical.TimeStamp interface to aid in
// metrics reporting.
func (t consistentPoint) AsTime() time.Time {
	if t.BackfillTime != nil {
		return *t.BackfillTime
	}
	return *t.StreamTime
}

// AsID is a convenience method to get the associated backfill document.
func (t consistentPoint) AsID() string {
	if t.BackfillID == nil {
		return ""
	}
	return *t.BackfillID
}

// MarshalText implements stamp.Stamp.
func (t consistentPoint) MarshalText() (text []byte, err error) {
	type payload consistentPoint
	p := payload(t)
	x, e := json.Marshal(p)
	return x, e
}

// Less implements stamp.Stamp.
func (t consistentPoint) Less(other stamp.Stamp) bool {
	o := other.(consistentPoint)

	tt := t.AsTime()
	ot := o.AsTime()
	if tt.Before(ot) {
		return true
	}
	if tt.After(ot) {
		return false
	}

	return strings.Compare(t.AsID(), o.AsID()) < 0
}

func backfillPoint(id string, ts time.Time) consistentPoint {
	return consistentPoint{
		BackfillID:   &id,
		BackfillTime: &ts,
	}
}

func streamPoint(ts time.Time) consistentPoint {
	return consistentPoint{
		StreamTime: &ts,
	}
}

// UnmarshalText implements TextUnmarshaler.
func (t *consistentPoint) UnmarshalText(data []byte) error {
	return json.Unmarshal(data, t)
}

var _ stamp.Stamp = consistentPoint{}

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

type backfillDoc struct {
	doc *firestore.DocumentSnapshot
}
type streamDoc struct {
	doc *firestore.DocumentSnapshot
}
type streamStart struct {
	readTime time.Time
}
type streamEnd struct{}
type streamDelete struct {
	doc *firestore.DocumentRef
}

func (d *Dialect) BackfillInto(
	ctx context.Context, ch chan<- logical.Message, state logical.State,
) error {
	// From will either be the update time of the last streamed record,
	// or the range that we were backfilling from before being
	// interrupted. If we were in the middle of backfilling, then we
	// also want to pick up from the last document that was processed.
	var from time.Time
	var fromID string
	if cp, ok := state.GetConsistentPoint().(consistentPoint); ok {
		from = cp.AsTime()
		fromID = cp.AsID()
	}

	for {
		to := time.Now()
		moreWork, err := d.backfillOnce(ctx, ch, to, &from, &fromID)
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
			continue
		}
		d.backfillDone = true
		log.Infof("finished backfilling collection %s", d.cfg.SourceCollection)
		return nil
	}
}

func (d *Dialect) backfillOnce(
	ctx context.Context,
	ch chan<- logical.Message,
	toExcl time.Time,
	lastReadTime *time.Time,
	lastReadID *string,
) (moreWork bool, _ error) {
	const limit = 2

	// Iterate over the collection by (updated_at, __doc_id__) using
	// a cursor-like approach so that we can checkpoint along the way.
	q := d.coll.
		OrderBy(d.cfg.UpdatedAtProperty.Raw(), firestore.Asc).
		OrderBy(firestore.DocumentID, firestore.Asc).
		Where(d.cfg.UpdatedAtProperty.Raw(), "<", toExcl).
		Limit(limit)
	if *lastReadID == "" {
		q = q.Where(d.cfg.UpdatedAtProperty.Raw(), ">=", *lastReadTime)
	} else {
		q = q.StartAfter(*lastReadTime, *lastReadID)
	}
	docs := q.Documents(ctx)
	defer docs.Stop()

	count := 0
	for {
		doc, err := docs.Next()
		if err == iterator.Done {
			return count == limit, nil
		}
		if err != nil {
			return false, errors.WithStack(err)
		}
		*lastReadID = doc.Ref.ID
		*lastReadTime, err = d.docUpdatedAt(doc)
		if err != nil {
			return false, err
		}
		select {
		case ch <- backfillDoc{doc}:
			count++
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
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
	cp := state.GetConsistentPoint().(consistentPoint)
	// Iterate over the collection by (updated_at, __doc_id__) using
	// a cursor-like approach so that we can checkpoint along the way.
	q := d.coll.
		OrderBy(d.cfg.UpdatedAtProperty.Raw(), firestore.Asc).
		StartAt(cp.AsTime())
	snaps := q.Snapshots(ctx)

	for {
		snap, err := snaps.Next()
		if err != nil {
			return errors.WithStack(err)
		}

		// Interruptable send of a new batch.
		select {
		case ch <- streamStart{snap.ReadTime}:
		case <-ctx.Done():
			return ctx.Err()
		}

		for _, change := range snap.Changes {
			switch change.Kind {
			case firestore.DocumentAdded,
				firestore.DocumentModified:
				select {
				case ch <- streamDoc{change.Doc}:
				case <-ctx.Done():
					return ctx.Err()
				}
			case firestore.DocumentRemoved:
				select {
				case ch <- streamDelete{change.Doc.Ref}:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}

		select {
		case ch <- streamEnd{}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

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
		case backfillDoc:
			doc := t.doc
			docUpdatedAt, err := d.docUpdatedAt(doc)
			if err != nil {
				return err
			}

			cp := backfillPoint(doc.Ref.ID, docUpdatedAt)
			mut, err := d.marshalMutation(doc, docUpdatedAt)
			if err != nil {
				return err
			}

			if err := events.OnBegin(ctx, cp); err != nil {
				return err
			}
			if err := events.OnData(ctx, d.cfg.TargetTable, []types.Mutation{mut}); err != nil {
				return err
			}
			if err := events.OnCommit(ctx); err != nil {
				return err
			}

		case streamStart:
			cp := streamPoint(t.readTime)
			if err := events.OnBegin(ctx, cp); err != nil {
				return err
			}

		case streamDoc:
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

		case streamEnd:
			if err := events.OnCommit(ctx); err != nil {
				return err
			}

		default:
			panic(fmt.Sprintf("unimplemented type %T", msg))
		}
	}
	return nil
}

func (d *Dialect) marshalMutation(doc *firestore.DocumentSnapshot, updatedAt time.Time) (types.Mutation, error) {
	// We want to bake the document id into the values to be
	// applied, with the assumption that it will be remapped (or
	// ignored) by the downstream apply rules.
	dataMap := doc.Data()
	dataMap[firestore.DocumentID] = doc.Ref.ID
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

// UnmarshalStamp implements logical.Dialect. It delegates to
// time.Time.UnmarshalText.
func (d *Dialect) UnmarshalStamp(bytes []byte) (stamp.Stamp, error) {
	var ts consistentPoint
	err := ts.UnmarshalText(bytes)
	return ts, err
}

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
