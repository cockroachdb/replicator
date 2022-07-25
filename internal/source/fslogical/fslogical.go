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

	log "github.com/sirupsen/logrus"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"

	"google.golang.org/api/iterator"

	"github.com/pkg/errors"

	"cloud.google.com/go/firestore"
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
)

type timeStamp struct {
	// The last document ID read when backfilling.
	BackfillID string `json:"k,omitempty"`
	// A server-generated timestamp; only updated when a backfill has completed
	// or when we receive new data in streaming mode.
	Time time.Time `json:"t"`
}

// AsTime implements the optional logical.TimeStamp interface to aid in
// metrics reporting.
func (t timeStamp) AsTime() time.Time {
	return t.Time
}

// MarshalText implements stamp.Stamp.
func (t timeStamp) MarshalText() (text []byte, err error) {
	return json.Marshal(t)
}

// Less implements stamp.Stamp.
func (t timeStamp) Less(other stamp.Stamp) bool {
	o := other.(timeStamp)
	return t.Time.Before(o.Time) || strings.Compare(t.BackfillID, o.BackfillID) < 0
}

// UnmarshalText implements TextUnmarshaler.
func (t *timeStamp) UnmarshalText(data []byte) error {
	return json.Unmarshal(data, t)
}

var _ stamp.Stamp = timeStamp{}

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
	ts  time.Time
	doc *firestore.DocumentSnapshot
}
type streamDoc struct {
	doc *firestore.DocumentSnapshot
}

func (d *Dialect) BackfillInto(
	ctx context.Context, ch chan<- logical.Message, state logical.State,
) error {
	var from time.Time
	var lastID string
	if cp, ok := state.GetConsistentPoint().(timeStamp); ok {
		from = cp.Time
		lastID = cp.BackfillID
	}

	for {
		to := time.Now()
		nextID, moreWork, err := d.backfillOnce(ctx, ch, from, to, lastID)
		if err != nil {
			return err
		}
		if moreWork {
			lastID = nextID
			continue
		}
		if time.Since(to) >= time.Minute {
			continue
		}
		d.backfillDone = true
		log.Infof("finished backfilling collection %s", d.cfg.SourceCollection)
		return nil
	}
}

func (d *Dialect) backfillOnce(
	ctx context.Context, ch chan<- logical.Message, from, to time.Time, start string,
) (lastID string, moreWork bool, _ error) {
	const limit = 2

	q := d.coll.
		OrderBy(d.cfg.UpdatedAtProperty.Raw(), firestore.Asc).
		OrderBy(firestore.DocumentID, firestore.Asc).
		Limit(limit).
		Where(d.cfg.UpdatedAtProperty.Raw(), "<=", to)
	if !from.IsZero() && start != "" {
		q = q.Where(d.cfg.UpdatedAtProperty.Raw(), ">", from).
			Where(d.)
	}

	docs := q.Documents(ctx)
	defer docs.Stop()

	count := 0
	for {
		doc, err := docs.Next()
		if err == iterator.Done {
			return lastID, count == limit, nil
		}
		lastID = doc.Ref.ID
		if err != nil {
			return lastID, false, errors.WithStack(err)
		}
		select {
		case ch <- backfillDoc{from, doc}:
			count++
		case <-ctx.Done():
			return lastID, false, ctx.Err()
		}
	}
}

// ShouldBackfill returns true if the backfill process has caught up
// to within one minute of the current time.
func (d *Dialect) ShouldBackfill(state logical.State) bool {
	return !d.backfillDone
}

func (d *Dialect) ReadInto(
	ctx context.Context, ch chan<- logical.Message, state logical.State,
) error {
	time.Sleep(time.Hour)
	// TODO implement me
	panic("implement me")
}

func (d *Dialect) Process(
	ctx context.Context, ch <-chan logical.Message, events logical.Events,
) error {
	var ts timeStamp
	for msg := range ch {
		if logical.IsRollback(msg) {
			if err := events.OnRollback(ctx, msg); err != nil {
				return err
			}
			continue
		}

		var doc *firestore.DocumentSnapshot
		switch t := msg.(type) {
		case backfillDoc:
			doc = t.doc
			ts.BackfillID = doc.Ref.ID
			ts.Time = t.ts

		case streamDoc:
			doc = t.doc
			x, err := doc.DataAt(d.cfg.UpdatedAtProperty.Raw())
			if err != nil {
				return errors.WithStack(err)
			}
			if now, ok := x.(time.Time); ok {
				ts.BackfillID = ""
				ts.Time = now
			} else {
				return errors.Errorf("could not find %s timestamp in %s", d.cfg.UpdatedAtProperty.Raw(), doc.Ref.ID)
			}

		default:
			panic(fmt.Sprintf("unimplemented type %T", msg))
		}

		data, err := json.Marshal(doc.Data())
		if err != nil {
			return errors.WithStack(err)
		}

		key, err := json.Marshal([]string{doc.Ref.ID})
		if err != nil {
			return errors.WithStack(err)
		}

		if err := events.OnBegin(ctx, ts); err != nil {
			return err
		}
		if err := events.OnData(ctx, d.cfg.TargetTable, []types.Mutation{{
			Data: data,
			Key:  key,
			Time: hlc.New(ts.Time.UnixNano(), 0),
		}}); err != nil {
			return err
		}
		if err := events.OnCommit(ctx); err != nil {
			return err
		}
	}
	return nil
}

// UnmarshalStamp implements logical.Dialect. It delegates to
// time.Time.UnmarshalText.
func (d *Dialect) UnmarshalStamp(bytes []byte) (stamp.Stamp, error) {
	var ts timeStamp
	err := ts.UnmarshalText(bytes)
	return ts, err
}
