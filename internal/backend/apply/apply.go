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
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/sinktypes"
	"github.com/cockroachdb/cdc-sink/internal/util/batches"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
)

// apply will upsert mutations and deletions into a target table.
type apply struct {
	cancel context.CancelFunc
	target ident.Table

	mu struct {
		sync.RWMutex
		columns []sinktypes.ColData
		pks     []sinktypes.ColData

		sql struct {
			// DELETE FROM t WHERE ("pk0", "pk1") IN (SELECT unnest($1::INT8[]), unnest($2::STRING[]))
			delete string
			// UPSERT INTO t ("pk0", "pk1") SELECT unnest($1::INT8[]), unnest($2::STRING[])
			upsert string
		}
	}
}

var _ sinktypes.Applier = (*apply)(nil)

// newApply constructs an apply by inspecting the target table.
func newApply(w sinktypes.Watcher, target ident.Table,
) (_ *apply, cancel func(), _ error) {
	ch, cancel, err := w.Watch(target)
	if err != nil {
		return nil, cancel, err
	}

	a := &apply{cancel: cancel, target: target}
	// Wait for the initial column data to be loaded.
	select {
	case colData := <-ch:
		a.refreshUnlocked(colData)
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
			log.Printf("refreshed schema for table %s", a.target)
		}
	}()

	return a, cancel, nil
}

// Apply applies the mutations to the target table.
func (a *apply) Apply(
	ctx context.Context, tx sinktypes.Batcher, muts []sinktypes.Mutation,
) error {
	deletes, r := batches.Mutation()
	defer r()
	upserts, r := batches.Mutation()
	defer r()

	a.mu.RLock()
	defer a.mu.RUnlock()

	if len(a.mu.columns) == 0 {
		return errors.Errorf("no ColumnData available for %s", a.target)
	}

	for i := range muts {
		if muts[i].Delete() {
			deletes = append(deletes, muts[i])
			if len(deletes) == cap(deletes) {
				if err := a.deleteLocked(ctx, tx, deletes); err != nil {
					return err
				}
				deletes = deletes[:0]
			}
		} else {
			upserts = append(upserts, muts[i])
			if len(upserts) == cap(upserts) {
				if err := a.upsertLocked(ctx, tx, upserts); err != nil {
					return err
				}
				upserts = upserts[:0]
			}
		}
	}

	if err := a.deleteLocked(ctx, tx, deletes); err != nil {
		return err
	}
	return a.upsertLocked(ctx, tx, upserts)
}

func (a *apply) deleteLocked(
	ctx context.Context, db sinktypes.Batcher, muts []sinktypes.Mutation,
) error {
	if len(muts) == 0 {
		return nil
	}

	batch := &pgx.Batch{}

	for i := range muts {
		dec := json.NewDecoder(bytes.NewReader(muts[i].Key))
		dec.UseNumber()

		args := make([]interface{}, 0, len(a.mu.pks))
		if err := dec.Decode(&args); err != nil {
			return errors.WithStack(err)
		}

		if len(args) != len(a.mu.pks) {
			return errors.Errorf(
				"schema drift detected: "+
					"inconsistent number of key colums: "+
					"received %d expect %d: "+
					"key %s@%s",
				len(args), len(a.mu.pks), string(muts[i].Key), muts[i].Time)
		}

		batch.Queue(a.mu.sql.delete, args...)
	}

	res := db.SendBatch(ctx, batch)
	defer res.Close()

	for i, j := 0, batch.Len(); i < j; i++ {
		_, err := res.Exec()
		if err != nil {
			return errors.Wrap(err, a.mu.sql.delete)
		}
	}

	return nil
}

func (a *apply) upsertLocked(
	ctx context.Context, db sinktypes.Batcher, muts []sinktypes.Mutation,
) error {
	if len(muts) == 0 {
		return nil
	}

	batch := &pgx.Batch{}

	for i := range muts {
		dec := json.NewDecoder(bytes.NewReader(muts[i].Data))
		dec.UseNumber()

		temp := make(map[string]interface{})
		if err := dec.Decode(&temp); err != nil {
			return errors.WithStack(err)
		}

		args := make([]interface{}, len(a.mu.columns))
		for colIdx := range a.mu.columns {
			rawColName := a.mu.columns[colIdx].Name.Raw()
			// We're not going to worry about missing columns in the
			// mutation to be applied unless it's a PK. If other new
			// columns have been added to the target table, the source
			// table might not have them yet.
			decoded, ok := temp[rawColName]
			if !ok && a.mu.columns[colIdx].Primary {
				return errors.Errorf(
					"schema drift detected in %s: "+
						"missing PK column %s: "+
						"key %s@%s",
					a.target, rawColName,
					string(muts[i].Key), muts[i].Time)
			}
			delete(temp, rawColName)
			args[colIdx] = decoded
		}
		batch.Queue(a.mu.sql.upsert, args...)

		// If new columns have been added in the source table, but not
		// in the destination, we want to error out.
		if len(temp) != 0 {
			var unexpected []string
			for k := range temp {
				unexpected = append(unexpected, k)
			}
			sort.Strings(unexpected)
			return errors.Errorf(
				"schema drift detected in %s: "+
					"unexpected columns %v: "+
					"key %s@%s",
				a.target, unexpected, string(muts[i].Key), muts[i].Time)
		}
	}

	res := db.SendBatch(ctx, batch)
	defer res.Close()

	for i, j := 0, batch.Len(); i < j; i++ {
		if _, err := res.Exec(); err != nil {
			return errors.Wrap(err, a.mu.sql.upsert)
		}
	}
	return nil
}

// refreshUnlocked updates the apply with new column information.
func (a *apply) refreshUnlocked(colData []sinktypes.ColData) {
	a.mu.Lock()
	defer a.mu.Unlock()

	var delete, upsert strings.Builder
	lastPkColumn := 0

	_, _ = fmt.Fprintf(&delete, "DELETE FROM %s WHERE (", a.target)
	_, _ = fmt.Fprintf(&upsert, "UPSERT INTO %s (", a.target)
	for i := range colData {
		if colData[i].Primary {
			if i > 0 {
				lastPkColumn = i
				delete.WriteString(", ")
			}
			delete.WriteString(colData[i].Name.String())
		}
		if i > 0 {
			upsert.WriteString(", ")
		}
		upsert.WriteString(colData[i].Name.String())
	}
	delete.WriteString(") IN (SELECT ")
	upsert.WriteString(") SELECT ")
	for i := range colData {
		if colData[i].Primary {
			if i > 0 {
				delete.WriteString(", ")
			}
			_, _ = fmt.Fprintf(&delete, "$%d::%s", i+1, colData[i].Type)
		}
		if i > 0 {
			upsert.WriteString(", ")
		}

		// The GEO types need some additional help to convert them from
		// the JSON-style representations that we get.
		switch colData[i].Type {
		case "GEOGRAPHY":
			_, _ = fmt.Fprintf(&upsert, "st_geogfromgeojson($%d::jsonb)", i+1)
		case "GEOMETRY":
			_, _ = fmt.Fprintf(&upsert, "st_geomfromgeojson($%d::jsonb)", i+1)
		default:
			_, _ = fmt.Fprintf(&upsert, "$%d::%s", i+1, colData[i].Type)
		}
	}
	delete.WriteString(")")

	a.mu.columns = colData
	a.mu.pks = colData[:lastPkColumn+1]
	a.mu.sql.delete = delete.String()
	a.mu.sql.upsert = upsert.String()
}
