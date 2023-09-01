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

// Package apply contains code for applying mutations to tables.
package apply

// This file contains code repackaged from sink.go.

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/staging/applycfg"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/batches"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/metrics"
	"github.com/cockroachdb/cdc-sink/internal/util/msort"
	"github.com/cockroachdb/cdc-sink/internal/util/pjson"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

// apply will upsert mutations and deletions into a target table.
type apply struct {
	product types.Product
	target  ident.Table

	deletes   prometheus.Counter
	durations prometheus.Observer
	errors    prometheus.Counter
	upserts   prometheus.Counter

	mu struct {
		sync.RWMutex
		templates *templates
	}
}

var _ types.Applier = (*apply)(nil)

// newApply constructs an apply by inspecting the target table.
func newApply(
	product types.Product, inTarget ident.Table, cfgs *applycfg.Configs, watchers types.Watchers,
) (_ *apply, cancel func(), _ error) {
	// Start a background goroutine to refresh the templates.
	ctx, cancel := context.WithCancel(context.Background())

	w, err := watchers.Get(ctx, inTarget.Schema())
	if err != nil {
		cancel()
		return nil, nil, err
	}
	// Use the target database's name for the table. We perform a lookup
	// against the column data map to retrieve the original table name
	// key under which the data was inserted.
	target, ok := w.Get().OriginalName(inTarget)
	if !ok {
		cancel()
		return nil, nil, errors.Errorf("unknown table %s", inTarget)
	}

	labelValues := metrics.TableValues(target)
	a := &apply{
		product: product,
		target:  target,

		deletes:   applyDeletes.WithLabelValues(labelValues...),
		durations: applyDurations.WithLabelValues(labelValues...),
		errors:    applyErrors.WithLabelValues(labelValues...),
		upserts:   applyUpserts.WithLabelValues(labelValues...),
	}

	errs := make(chan error, 1)
	go func(ctx context.Context, errs chan<- error) {
		defer cancel()

		schemaCh, cancelSchema, err := w.Watch(target)
		if err != nil {
			errs <- err
			return
		}
		defer cancelSchema()

		configCh, cancelConfig := cfgs.Watch(target)
		if err != nil {
			errs <- err
			return
		}
		defer cancelConfig()

		var configData *applycfg.Config
		var schemaData []types.ColData
		for {
			select {
			case <-ctx.Done():
				return
			case configData = <-configCh:
			case schemaData = <-schemaCh:
			}
			if configData != nil && len(schemaData) > 0 {
				err := a.refreshUnlocked(configData, schemaData)
				if err != nil {
					log.WithError(err).WithField("table", target).Warn("could not refresh table metadata")
				}
				// Send the first error (or nil) to the channel, then
				// close it. Shut down if we get an error at the outset.
				if errs != nil {
					errs <- err
					close(errs)
					errs = nil
					if err != nil {
						return
					}
				}
			}
		}
	}(ctx, errs)

	// Wait for the first loop of the refresh goroutine above.
	if err := <-errs; err != nil {
		return nil, nil, err
	}

	return a, cancel, nil
}

// Apply applies the mutations to the target table.
func (a *apply) Apply(ctx context.Context, tx types.TargetQuerier, muts []types.Mutation) error {
	start := time.Now()
	deletes, r := batches.Mutation()
	defer r()
	upserts, r := batches.Mutation()
	defer r()

	// We want to ensure that we achieve a last-one-wins behavior within
	// an immediate-mode batch. This does perform unnecessary work
	// in the staged mode, since we perform the per-key deduplication
	// and sorting as part of de-queuing mutations.
	//
	// See also the discussion on TestRepeatedKeysWithIgnoredColumns
	muts = msort.UniqueByKey(muts)

	countError := func(err error) error {
		if err != nil {
			a.errors.Inc()
		}
		return err
	}

	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.mu.templates.Positions.Len() == 0 {
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

func (a *apply) deleteLocked(
	ctx context.Context, db types.TargetQuerier, muts []types.Mutation,
) error {
	if len(muts) == 0 {
		return nil
	}
	sql, err := a.mu.templates.deleteExpr(len(muts))
	if err != nil {
		return err
	}

	keyGroups := make([][]any, len(muts))
	if err := pjson.Decode(ctx, keyGroups, func(i int) []byte {
		return muts[i].Key
	}); err != nil {
		return err
	}

	allArgs := make([]any, 0, len(a.mu.templates.PKDelete)*len(muts))
	for i, keyGroup := range keyGroups {
		if len(keyGroup) != len(a.mu.templates.PKDelete) {
			return errors.Errorf(
				"schema drift detected in %s: "+
					"inconsistent number of key columns: "+
					"received %d expect %d: "+
					"key %s@%s",
				a.target,
				len(keyGroup), len(a.mu.templates.PKDelete),
				string(muts[i].Key), muts[i].Time)
		}
		allArgs = append(allArgs, keyGroup...)
	}

	for idx, arg := range allArgs {
		if num, ok := arg.(json.Number); ok {
			// See comment in upsertLocked().
			allArgs[idx] = removeExponent(num).String()
		}
	}

	tag, err := db.ExecContext(ctx, sql, allArgs...)
	if err != nil {
		return errors.Wrap(err, sql)
	}
	affected, err := tag.RowsAffected()
	if err != nil {
		return errors.WithStack(err)
	}

	a.deletes.Add(float64(affected))
	log.WithFields(log.Fields{
		"applied":  affected,
		"proposed": len(muts),
		"target":   a.target,
	}).Debug("deleted rows")
	return nil
}

func (a *apply) upsertLocked(
	ctx context.Context, db types.TargetQuerier, muts []types.Mutation,
) error {
	if len(muts) == 0 {
		return nil
	}
	start := time.Now()

	sql, err := a.mu.templates.upsertExpr(len(muts))
	if err != nil {
		return err
	}

	// Allocate a slice for all mutation data. We'll reset the length
	// once we know how many elements we actually have.
	allArgs := make([]any, a.mu.templates.UpsertParameterCount*len(muts))
	argIdx := 0

	// Decode the mutations into an actionable map.
	allPayloadData := make([]*ident.Map[any], len(muts))
	if err := pjson.Decode(ctx, allPayloadData,
		func(i int) []byte { return muts[i].Data },
	); err != nil {
		return err
	}

	for idx, rowData := range allPayloadData {
		var extrasData *ident.Map[any]
		if a.mu.templates.ExtrasColIdx != -1 {
			extrasData = &ident.Map[any]{}
		}

		// Track the columns that we must see and that are seen in
		// the incoming payload. This improves the error returned when
		// there are unexpected columns.
		missingPKs := &ident.Map[struct{}]{}
		for _, pk := range a.mu.templates.PK {
			missingPKs.Put(pk.Name, struct{}{})
		}

		var unexpectedColumns []string

		err = rowData.Range(func(incomingColName ident.Ident, value any) error {
			targetColumn, ok := a.mu.templates.Positions.Get(incomingColName)

			// The incoming data didn't map to a column. Report an error
			// if no extras column has been configured or accumulate it
			// for later encoding.
			if !ok {
				if extrasData == nil {
					unexpectedColumns = append(unexpectedColumns, incomingColName.Raw())
				} else {
					extrasData.Put(incomingColName, value)
				}
				return nil
			}

			// We've seen the PK column, remove it from the set of
			// unseen names.
			if targetColumn.Primary {
				missingPKs.Delete(targetColumn.Name)
			}

			// This is a signal that we want the column to be mentioned
			// in the input data, but that we don't actually want to
			// make use of the provided value. For example, generated PK
			// columns.
			if targetColumn.UpsertIndex < 0 {
				return nil
			}

			// The JSON parser is configured to parse numbers as though
			// they were strings.  We'll keep the string encoding so
			// that the target database can tell us if the string value
			// exceeds the precision or scale for the target column.
			if num, ok := value.(json.Number); ok {
				value = removeExponent(num).String()
			} else if str, ok := value.(string); ok && targetColumn.Parse != nil {
				value, ok = targetColumn.Parse(str)
				if !ok {
					return errors.Errorf("could not parse %q as a %s", str, targetColumn.Type)
				}
			}

			// This loop is driven by the fields in the incoming
			// payload, so we can just set the validity flag (if any) to
			// a non-null value.
			if targetColumn.ValidityIndex >= 0 {
				allArgs[argIdx+targetColumn.ValidityIndex] = true
			}

			// Assign the value to the relevant offset in the args.
			allArgs[argIdx+targetColumn.UpsertIndex] = value
			return nil
		})
		if err != nil {
			return err
		}

		// Report missing PK columns. We're not going to worry about
		// other missing columns in the mutation to be applied. If other
		// new columns have been added to the target table, the source
		// table might not have them yet.
		if missingPKs.Len() > 0 {
			var missingCols strings.Builder
			_ = missingPKs.Range(func(name ident.Ident, _ struct{}) error {
				if missingCols.Len() > 0 {
					missingCols.WriteString(", ")
				}
				missingCols.WriteString(name.Raw())
				return nil
			})
			return errors.Errorf(
				"schema drift detected in %s: "+
					"missing PK column %s: "+
					"key %s@%s",
				a.target, missingCols.String(),
				string(muts[idx].Key), muts[idx].Time)
		}

		// Report any other unmapped column names.
		if len(unexpectedColumns) > 0 {
			return errors.Errorf(
				"schema drift detected in %s: "+
					"unexpected columns %v: "+
					"key %s@%s",
				a.target, unexpectedColumns, string(muts[idx].Key), muts[idx].Time)
		}

		if extrasData != nil {
			extraJSONBytes, err := json.Marshal(extrasData)
			if err != nil {
				return errors.Wrap(err, "could not encode extras column value")
			}
			allArgs[argIdx+a.mu.templates.ExtrasColIdx] = string(extraJSONBytes)
		}

		argIdx += a.mu.templates.UpsertParameterCount
	}
	allArgs = allArgs[:argIdx]

	tag, err := db.ExecContext(ctx, sql, allArgs...)
	if err != nil {
		return errors.Wrap(err, sql)
	}
	affected, err := tag.RowsAffected()
	if err != nil {
		return errors.WithStack(err)
	}

	a.upserts.Add(float64(affected))
	log.WithFields(log.Fields{
		"applied":  affected,
		"duration": time.Since(start),
		"proposed": len(muts),
		"target":   a.target,
	}).Debug("upserted rows")
	return nil
}

// refreshUnlocked updates the apply with new column information.
func (a *apply) refreshUnlocked(configData *applycfg.Config, schemaData []types.ColData) error {
	// Sanity-check the configuration against target schema.
	if err := a.validate(configData, schemaData); err != nil {
		return err
	}

	// Extract column metadata.
	columnMapping, err := newColumnMapping(configData, schemaData, a.product, a.target)
	if err != nil {
		return err
	}

	// Build template cache.
	tmpl, err := newTemplates(columnMapping)
	if err != nil {
		return err
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	a.mu.templates = tmpl
	return nil
}

func (a *apply) validate(configData *applycfg.Config, schemaData []types.ColData) error {
	// We want to verify that the cas and deadline columns actually
	// exist in the incoming column data.
	var allColNames ident.Map[struct{}]
	for _, col := range schemaData {
		allColNames.Put(col.Name, struct{}{})
	}
	for _, col := range configData.CASColumns {
		if _, found := allColNames.Get(col); !found {
			return errors.Errorf("cas column name %s not found in table %s", col, a.target)
		}
	}
	if err := configData.Deadlines.Range(func(col ident.Ident, _ time.Duration) error {
		if _, found := allColNames.Get(col); !found {
			return errors.Errorf("deadline column name %s not found in table %s", col, a.target)
		}
		return nil
	}); err != nil {
		return err
	}
	if err := configData.Exprs.Range(func(col ident.Ident, _ string) error {
		if _, found := allColNames.Get(col); !found {
			return errors.Errorf("expression column name %s not found in table %s", col, a.target)
		}
		return nil
	}); err != nil {
		return err
	}
	if !configData.Extras.Empty() {
		if _, found := allColNames.Get(configData.Extras); !found {
			return errors.Errorf("extras column name %s not found in table %s",
				configData.Extras, a.target)
		}
	}

	// The Ignores field doesn't need validation, since you might want
	// to mark a column as ignored in order to (eventually) drop it from
	// the destination database.

	if err := configData.SourceNames.Range(func(col ident.Ident, _ applycfg.SourceColumn) error {
		if _, found := allColNames.Get(col); !found {
			return errors.Errorf("renamed column name %s not found in table %s", col, a.target)
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}
