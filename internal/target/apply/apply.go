// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package apply contains code for applying mutations to tables.
package apply

// This file contains code repackaged from sink.go.

import (
	"bytes"
	"context"
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/batches"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/metrics"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

// apply will upsert mutations and deletions into a target table.
type apply struct {
	cancel     context.CancelFunc
	casColumns []ident.Ident
	deadlines  types.Deadlines
	target     ident.Table

	deletes   prometheus.Counter
	durations prometheus.Observer
	errors    prometheus.Counter
	upserts   prometheus.Counter

	mu struct {
		sync.RWMutex
		columns   []types.ColData
		pks       []types.ColData
		templates *templates
	}
}

var _ types.Applier = (*apply)(nil)

// newApply constructs an apply by inspecting the target table.
func newApply(
	w types.Watcher, target ident.Table, casColumns []ident.Ident, deadlines types.Deadlines,
) (_ *apply, cancel func(), _ error) {
	ch, cancel, err := w.Watch(target)
	if err != nil {
		return nil, cancel, err
	}

	labelValues := metrics.TableValues(target)
	a := &apply{
		cancel:     cancel,
		casColumns: casColumns,
		deadlines:  deadlines,
		target:     target,

		deletes:   applyDeletes.WithLabelValues(labelValues...),
		durations: applyDurations.WithLabelValues(labelValues...),
		errors:    applyErrors.WithLabelValues(labelValues...),
		upserts:   applyUpserts.WithLabelValues(labelValues...),
	}

	// Wait for the initial column data to be loaded.
	select {
	case colData := <-ch:
		if err := a.refreshUnlocked(colData); err != nil {
			return nil, cancel, err
		}
	case <-time.After(10 * time.Second):
		return nil, cancel, errors.Errorf("column data timeout for %s", target)
	}

	// Background routine to keep the column data refreshed.
	go func() {
		for {
			colData, open := <-ch
			if !open {
				return
			}
			a.refreshUnlocked(colData)
			log.WithField("table", a.target).Debug("refreshed schema")
		}
	}()

	return a, cancel, nil
}

// Apply applies the mutations to the target table.
func (a *apply) Apply(ctx context.Context, tx pgxtype.Querier, muts []types.Mutation) error {
	start := time.Now()
	deletes, r := batches.Mutation()
	defer r()
	upserts, r := batches.Mutation()
	defer r()

	countError := func(err error) error {
		if err != nil {
			a.errors.Inc()
		}
		return err
	}

	a.mu.RLock()
	defer a.mu.RUnlock()

	if len(a.mu.columns) == 0 {
		return errors.Errorf("no ColumnData available for %s", a.target)
	}

	for i := range muts {
		if muts[i].IsDelete() {
			deletes = append(deletes, muts[i])
			if len(deletes) == cap(deletes) {
				if err := a.deleteLocked(ctx, tx, deletes); err != nil {
					return countError(err)
				}
				deletes = deletes[:0]
			}
		} else {
			upserts = append(upserts, muts[i])
			if len(upserts) == cap(upserts) {
				if err := a.upsertLocked(ctx, tx, upserts); err != nil {
					return countError(err)
				}
				upserts = upserts[:0]
			}
		}
	}

	if err := a.deleteLocked(ctx, tx, deletes); err != nil {
		return countError(err)
	}
	if err := a.upsertLocked(ctx, tx, upserts); err != nil {
		return countError(err)
	}
	a.durations.Observe(time.Since(start).Seconds())
	return nil
}

func (a *apply) deleteLocked(ctx context.Context, db pgxtype.Querier, muts []types.Mutation) error {
	if len(muts) == 0 {
		return nil
	}
	sql, err := a.mu.templates.delete(len(muts))
	if err != nil {
		return err
	}

	allArgs := make([]interface{}, 0, len(a.mu.pks)*len(muts))

	for i := range muts {
		dec := json.NewDecoder(bytes.NewReader(muts[i].Key))
		dec.UseNumber()

		args := make([]interface{}, 0, len(a.mu.pks))
		if err := dec.Decode(&args); err != nil {
			return errors.WithStack(err)
		}

		if len(args) != len(a.mu.pks) {
			return errors.Errorf(
				"schema drift detected in %s: "+
					"inconsistent number of key columns: "+
					"received %d expect %d: "+
					"key %s@%s",
				a.target,
				len(args), len(a.mu.pks),
				string(muts[i].Key), muts[i].Time)
		}
		allArgs = append(allArgs, args...)
	}

	tag, err := db.Exec(ctx, sql, allArgs...)
	if err != nil {
		return errors.Wrap(err, sql)
	}

	a.deletes.Add(float64(tag.RowsAffected()))
	log.WithFields(log.Fields{
		"applied":  tag.RowsAffected(),
		"proposed": len(muts),
		"target":   a.target,
	}).Debug("deleted rows")
	return nil
}

func (a *apply) upsertLocked(ctx context.Context, db pgxtype.Querier, muts []types.Mutation) error {
	if len(muts) == 0 {
		return nil
	}
	sql, err := a.mu.templates.upsert(len(muts))
	if err != nil {
		return err
	}

	allArgs := make([]interface{}, 0, len(a.mu.columns)*len(muts))

	for i := range muts {
		dec := json.NewDecoder(bytes.NewReader(muts[i].Data))
		dec.UseNumber()

		incomingColumnData := make(map[string]interface{})
		if err := dec.Decode(&incomingColumnData); err != nil {
			return errors.WithStack(err)
		}

		// The values to pass to the database.
		args := make([]interface{}, 0, len(a.mu.columns))
		// Track the columns that we expect to see and that are seen in
		// the incoming payload. This improves the error returned when
		// there are unexpected columns.
		knownColumnsInPayload := make(map[string]struct{}, len(a.mu.columns))

		for _, col := range a.mu.columns {
			rawColName := col.Name.Raw()
			decoded, presentInPayload := incomingColumnData[rawColName]
			// Keep track of columns in the incoming payload that match
			// columns that we expect to see in the target database.
			if presentInPayload {
				knownColumnsInPayload[rawColName] = struct{}{}
			}
			// Ignored will be true for columns in the target database
			// that we know about, but that we don't actually want to
			// insert new values for (e.g. computed columns). These
			// ignored columns could be part of the primary key, or they
			// could be a regular column.
			if col.Ignored {
				continue
			}
			// We're not going to worry about missing columns in the
			// mutation to be applied unless it's a PK. If other new
			// columns have been added to the target table, the source
			// table might not have them yet.
			if col.Primary && !presentInPayload {
				return errors.Errorf(
					"schema drift detected in %s: "+
						"missing PK column %s: "+
						"key %s@%s",
					a.target, rawColName,
					string(muts[i].Key), muts[i].Time)
			}
			args = append(args, decoded)
		}
		allArgs = append(allArgs, args...)

		// If new columns have been added in the source table, but not
		// in the destination, we want to error out.
		if len(incomingColumnData) > len(knownColumnsInPayload) {
			var unexpected []string
			for key := range incomingColumnData {
				if _, seen := knownColumnsInPayload[key]; !seen {
					unexpected = append(unexpected, key)
				}
			}
			sort.Strings(unexpected)
			return errors.Errorf(
				"schema drift detected in %s: "+
					"unexpected columns %v: "+
					"key %s@%s",
				a.target, unexpected, string(muts[i].Key), muts[i].Time)
		}
	}

	tag, err := db.Exec(ctx, sql, allArgs...)
	if err != nil {
		return errors.Wrap(err, sql)
	}

	a.upserts.Add(float64(tag.RowsAffected()))
	log.WithFields(log.Fields{
		"applied":  tag.RowsAffected(),
		"proposed": len(muts),
		"target":   a.target,
	}).Debug("upserted rows")
	return nil
}

// refreshUnlocked updates the apply with new column information.
func (a *apply) refreshUnlocked(colData []types.ColData) error {
	// We want to verify that the cas and deadline columns actually
	// exist in the incoming column data.
	allColNames := make(map[ident.Ident]struct{}, len(colData))
	for _, col := range colData {
		allColNames[col.Name] = struct{}{}
	}
	for _, col := range a.casColumns {
		if _, found := allColNames[col]; !found {
			return errors.Errorf("cas column name %s not found in table %s", col, a.target)
		}
	}
	for col := range a.deadlines {
		if _, found := allColNames[col]; !found {
			return errors.Errorf("deadline column name %s not found in table %s", col, a.target)
		}
	}

	tmpls := newTemplates(a.target, a.casColumns, a.deadlines, colData)

	a.mu.Lock()
	defer a.mu.Unlock()
	a.mu.columns = colData
	a.mu.pks = tmpls.PK
	a.mu.templates = tmpls
	return nil
}
