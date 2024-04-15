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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/applycfg"
	"github.com/cockroachdb/cdc-sink/internal/util/batches"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/merge"
	"github.com/cockroachdb/cdc-sink/internal/util/metrics"
	"github.com/cockroachdb/cdc-sink/internal/util/msort"
	"github.com/cockroachdb/cdc-sink/internal/util/notify"
	"github.com/cockroachdb/cdc-sink/internal/util/pjson"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

// apply will upsert mutations and deletions into a target table.
type apply struct {
	cache   *types.TargetStatements
	dlqs    types.DLQs
	product types.Product
	target  *ident.Hinted[ident.Table]

	ages      prometheus.Observer
	conflicts prometheus.Counter
	deletes   prometheus.Counter
	durations prometheus.Observer
	errors    prometheus.Counter
	resolves  prometheus.Counter
	upserts   prometheus.Counter

	mu struct {
		sync.RWMutex
		bagSpec   *merge.BagSpec
		gen       int // Use for prepared-statement cache invalidation.
		templates *templates
	}
}

type applyMode bool

const (
	applyConditional   applyMode = false // Use conditional apply and enable merge, if defined.
	applyUnconditional applyMode = true  // Don't use CAS support, just apply the data.
)

// newApply constructs an apply by inspecting the target table.
func (f *factory) newApply(
	ctx *stopper.Context, poolInfo *types.PoolInfo, inTarget ident.Table,
) (*apply, error) {
	w, err := f.watchers.Get(inTarget.Schema())
	if err != nil {
		return nil, err
	}
	// Use the target database's name for the table. We perform a lookup
	// against the column data map to retrieve the original table name
	// key under which the data was inserted.
	target, ok := w.Get().OriginalName(inTarget)
	if !ok {
		return nil, errors.Errorf("unknown table %s", inTarget)
	}

	labelValues := metrics.TableValues(target)
	a := &apply{
		cache:   f.cache,
		dlqs:    f.dlqs,
		product: poolInfo.Product,
		target:  poolInfo.HintNoFTS(target),

		ages:      applyMutationAge.WithLabelValues(labelValues...),
		conflicts: applyConflicts.WithLabelValues(labelValues...),
		deletes:   applyDeletes.WithLabelValues(labelValues...),
		durations: applyDurations.WithLabelValues(labelValues...),
		errors:    applyErrors.WithLabelValues(labelValues...),
		resolves:  applyResolves.WithLabelValues(labelValues...),
		upserts:   applyUpserts.WithLabelValues(labelValues...),
	}

	configHandle := f.configs.Get(target)
	configData, configChanged := configHandle.Get()

	if configData.Merger != nil && !IsMergeSupported(a.product) {
		return nil, errors.Errorf("merge operation not implemented for %s", a.product)
	}

	var initialErr notify.Var[error]
	_, ready := initialErr.Get()
	ctx.Go(func() error {
		schemaCh, cancelSchema, err := w.Watch(target)
		if err != nil {
			return err
		}
		defer cancelSchema()
		initial := true
		var schemaData []types.ColData
		for {
			select {
			case <-ctx.Stopping():
				return nil
			case <-configChanged:
				configData, configChanged = configHandle.Get()
			case schemaData = <-schemaCh:
			}

			if len(schemaData) > 0 {
				err := a.refreshUnlocked(configData, schemaData)
				if initial {
					// If we're just starting up, return errors.
					initialErr.Set(errors.Wrap(err, "could not read table metadata"))
					initial = false
				} else if err != nil {
					// In the steady state, just log refresh errors.
					log.WithError(err).WithField("table", target).Warn("could not refresh table metadata")
				}
			}
		}
	})

	select {
	case <-ready:
		// Wait for the first loop of the refresh goroutine above.
		err, _ = initialErr.Get()
	case <-ctx.Stopping():
		err = errors.New("stopping before initial schema read")
	}
	return a, err
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

	// Accumulate mutations and flush incrementally.
	for i := range muts {
		if muts[i].IsDelete() {
			deletes = append(deletes, muts[i])
			if len(deletes) == cap(deletes) {
				if err := a.deleteLocked(ctx, tx, deletes); err != nil {
					return countError(err)
				}
				deletes = deletes[:0]
			}
		} else if custom, ok := muts[i].Meta[types.CustomUpsert]; ok {
			// Flush
			if err := a.upsertLocked(ctx, tx, upserts, ""); err != nil {
				return countError(err)
			}
			upserts = upserts[:0]
			template, ok := custom.(string)
			if !ok {
				return errors.Errorf("invalid value for Meta[%s]", types.CustomUpsert)
			}
			// Apply custom template on its own.
			if err := a.upsertLocked(ctx, tx, []types.Mutation{muts[i]}, template); err != nil {
				return countError(err)
			}
		} else {
			upserts = append(upserts, muts[i])
			if len(upserts) == cap(upserts) {
				if err := a.upsertLocked(ctx, tx, upserts, ""); err != nil {
					return countError(err)
				}
				upserts = upserts[:0]
			}
		}
	}

	// Final flush.
	if err := a.deleteLocked(ctx, tx, deletes); err != nil {
		return countError(err)
	}
	if err := a.upsertLocked(ctx, tx, upserts, ""); err != nil {
		return countError(err)
	}

	endNanos := time.Now().UnixNano()
	a.durations.Observe(time.Duration(endNanos - start.UnixNano()).Seconds())
	for _, mut := range muts {
		a.ages.Observe(time.Duration(endNanos - mut.Time.Nanos()).Seconds())
	}
	return nil
}

func (a *apply) deleteLocked(
	ctx context.Context, db types.TargetQuerier, muts []types.Mutation,
) error {
	if len(muts) == 0 {
		return nil
	}
	pkCols := a.mu.templates.PKDelete
	keyGroups := make([][]any, len(muts))
	if err := pjson.Decode(ctx, keyGroups, func(i int) []byte {
		return muts[i].Key
	}); err != nil {
		return err
	}

	allArgs := make([]any, 0, len(pkCols)*len(muts))
	for i, keyGroup := range keyGroups {
		if len(keyGroup) != len(pkCols) {
			return errors.Errorf(
				"schema drift detected in %s: "+
					"inconsistent number of key columns: "+
					"received %d expect %d: "+
					"key %s@%s",
				a.target,
				len(keyGroup), len(pkCols),
				string(muts[i].Key), muts[i].Time)
		}

		// See discussion in upsertArgsLocked.
		if err := toDBTypes(pkCols, keyGroup); err != nil {
			return err
		}

		allArgs = append(allArgs, keyGroup...)
	}

	// The statement and its cache key will vary if the target supports
	// bulk deletions. In bulk mode, the number of rows does not impact
	// the generated SQL, since we rely on the driver to transfer
	// multiple arrays of column values.
	var stmtCacheKey string
	if a.mu.templates.BulkDelete {
		var err error
		allArgs, err = toColumns(len(a.mu.templates.PKDelete), len(muts), allArgs)
		if err != nil {
			return err
		}
		stmtCacheKey = fmt.Sprintf("delete-%s-%d", a.target, a.mu.gen)
	} else {
		stmtCacheKey = fmt.Sprintf("delete-%s-%d-%d", a.target, a.mu.gen, len(muts))
	}
	stmt, err := a.cache.Prepare(ctx,
		db,
		stmtCacheKey,
		func() (string, error) {
			return a.mu.templates.deleteExpr(len(muts))
		})
	if err != nil {
		return err
	}

	tag, err := stmt.ExecContext(ctx, allArgs...)
	if err != nil {
		return errors.WithStack(err)
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

// upsertLocked decodes the mutations into property bags and calls
// upsertBagsLocked.
func (a *apply) upsertLocked(
	ctx context.Context, db types.TargetQuerier, muts []types.Mutation, template string,
) error {
	// Decode the mutations into schema-aware property bags. These will
	// group property values into those known in the schema, versus
	// unknown (extra) properties.
	allPayloadData := make([]*merge.Bag, len(muts))
	if err := pjson.Decode(ctx, allPayloadData,
		func(i int) []byte {
			allPayloadData[i] = a.newBagLocked()
			return muts[i].Data
		},
	); err != nil {
		return err
	}
	return a.upsertBagsLocked(ctx, db, applyConditional, muts, allPayloadData, template)
}

// upsertArgsLocked shuffles the contents of the property bags into the
// arguments that will be passed to the SQL command.
func (a *apply) upsertArgsLocked(bags []*merge.Bag) ([]any, error) {
	// Allocate a slice for all mutation data. We'll reset the length
	// once we know how many elements we actually have.
	allArgs := make([]any, a.mu.templates.UpsertParameterCount*len(bags))
	argIdx := 0

	for idx, rowData := range bags {
		// Report missing PK columns. We're not going to worry about
		// other missing columns in the mutation to be applied. If other
		// new columns have been added to the target table, the source
		// table might not have them yet.
		if err := merge.ValidatePK(rowData); err != nil {
			return nil, errors.Wrapf(err, "schema drift detected in %s at "+
				"payload object offset %d", a.target, idx)
		}

		// Assign each mapped value to the argument slice we're going to
		// send to the database.
		if err := rowData.Mapped.Range(func(colName ident.Ident, entry *merge.Entry) error {
			// This is where we perform any last-minute, target-specific
			// fixes to reified values. That is, if the target database
			// requires special formatting or other data encapsulation,
			// this is the place to do it.
			value, err := toDBType(entry.Column, entry.Value)
			if err != nil {
				return err
			}

			// Now that we know what value we're inserting, we need to
			// look up where it goes in the output slice.
			targetColumn, ok := a.mu.templates.Positions.Get(colName)
			if !ok {
				// This would represent a coding error, not an
				// input-validation problem.
				return errors.Errorf("mapped column %s in property bag "+
					"could not be found in template positions map", colName)
			}

			// This is a signal that we want the column to be mentioned
			// in the input data, but that we don't actually want to
			// make use of the provided value. For example, generated PK
			// columns or ignored columns.
			if targetColumn.UpsertIndex < 0 {
				return nil
			}

			// This loop is driven by the fields in the incoming
			// payload, so we can just set the validity flag (if any) to
			// a non-null value.
			if entry.Valid && targetColumn.ValidityIndex >= 0 {
				allArgs[argIdx+targetColumn.ValidityIndex] = 1
			}

			// Assign the value to the relevant offset in the args.
			allArgs[argIdx+targetColumn.UpsertIndex] = value

			return nil
		}); err != nil {
			return nil, err
		}

		// Drop any ignored columns.
		for _, ignored := range a.mu.templates.Ignore {
			rowData.Delete(ignored)
		}

		// Handle any properties that don't map to the schema.
		if extraIdx := a.mu.templates.ExtrasColIdx; extraIdx == -1 {
			// Report unmapped properties as an error if there's nowhere
			// to store the data.
			if err := merge.ValidateNoUnmappedColumns(rowData); err != nil {
				return nil, errors.Wrapf(err,
					"schema drift detected in %s at payload object offset %d", a.target, idx)
			}
		} else {
			extraJSONBytes, err := json.Marshal(&rowData.Unmapped)
			if err != nil {
				return nil, errors.Wrap(err, "could not encode extras column value")
			}
			allArgs[argIdx+extraIdx] = string(extraJSONBytes)
		}

		argIdx += a.mu.templates.UpsertParameterCount
	}
	allArgs = allArgs[:argIdx]

	// Pivot to columnar data layout if the target supports a
	// bulk-transfer statement.
	if a.mu.templates.BulkUpsert {
		var err error
		allArgs, err = toColumns(a.mu.templates.UpsertParameterCount, len(bags), allArgs)
		if err != nil {
			return nil, err
		}
	}
	return allArgs, nil
}

// upsertBagsLocked contains the apply/merge functionality. The bags
// argument provides the reified data to insert into the database. The
// muts argument is only used if a merge behavior is required.
//
// This method will recurse into itself once if a merge function
// supplies de-conflicted rows.
func (a *apply) upsertBagsLocked(
	ctx context.Context,
	db types.TargetQuerier,
	mode applyMode,
	muts []types.Mutation,
	bags []*merge.Bag,
	template string,
) error {
	// Don't test len(muts), we discard them when applying merged data.
	if len(bags) == 0 {
		return nil
	}
	start := time.Now()

	// Converts the property bags into the expected argument layout.
	allArgs, err := a.upsertArgsLocked(bags)
	if err != nil {
		return err
	}

	// Get a prepared statement handle that's attached to the
	// transaction. The statement and its cache key will vary if the
	// target supports bulk upserts. In bulk mode, the number of rows
	// does not impact the generated SQL, since we rely on the driver to
	// transfer multiple arrays of column values.
	var stmtCacheKey string
	if a.mu.templates.BulkUpsert {
		stmtCacheKey = fmt.Sprintf("upsert-%s-%d-%v-%s", a.target, a.mu.gen, mode, template)
	} else {
		stmtCacheKey = fmt.Sprintf("upsert-%s-%d-%d-%v-%s", a.target, a.mu.gen, len(bags), mode, template)
	}
	stmt, err := a.cache.Prepare(ctx,
		db,
		stmtCacheKey,
		func() (string, error) {
			if template != "" {
				// We only support applyUnconditional with custom templates.
				mode = applyUnconditional
				return a.mu.templates.customExpr(len(bags), template, mode)
			}
			return a.mu.templates.upsertExpr(len(bags), mode)
		})
	if err != nil {
		return err
	}

	merger := a.mu.templates.Merger

	if template != "" && merger != nil {
		return errors.New("merge not supported with custom templates")
	}
	// If no merge function is defined or if we're forcing upserts,
	// we'll just execute the statement and return.
	if mode == applyUnconditional || merger == nil {
		// There's no merge behavior, so we don't need to read anything back.
		tag, err := stmt.ExecContext(ctx, allArgs...)
		if err != nil {
			return errors.WithStack(err)
		}
		upserted, err := tag.RowsAffected()
		if err != nil {
			return errors.WithStack(err)
		}
		// Conflicts could be non-zero if CAS is enabled and there's no
		// merge function. If CAS is not enabled, or we're applying
		// unconditionally, we expect to see all rows upserted and this
		// value will be zero.
		conflicts := int64(len(bags)) - upserted
		// MySQL RowsAffected value per row is 1 if the row is inserted
		// as a new row, 2 if an existing row is updated
		// so conflicts could be < 0 - adding a test to prevent
		// panic on the prometheus counter.
		if conflicts < 0 {
			conflicts = 0
		}
		a.conflicts.Add(float64(conflicts))
		a.upserts.Add(float64(upserted))
		log.WithFields(log.Fields{
			"conflicts": conflicts,
			"duration":  time.Since(start),
			"proposed":  len(bags),
			"target":    a.target,
			"upserted":  upserted,
		}).Debug("upserted rows")
		return nil
	}

	conflictingRows, err := stmt.QueryContext(ctx, allArgs...)
	if err != nil {
		return errors.WithStack(err)
	}
	defer conflictingRows.Close()

	// Read the conflicting rows back to generate the conflicts to resolve.
	var conflicts []*merge.Conflict
	var conflictMuts []types.Mutation
	for conflictingRows.Next() {
		// Index into the muts slice.
		var sourceIdx int
		// Columns in the blocking row.
		blockingData := make([]any, len(a.mu.templates.Columns))

		// Pointers into the blockingData slice.
		scanPtrs := make([]any, len(blockingData)+1)
		scanPtrs[0] = &sourceIdx
		for i := range blockingData {
			scanPtrs[i+1] = &blockingData[i]
		}
		if err := conflictingRows.Scan(scanPtrs...); err != nil {
			return errors.WithStack(err)
		}

		// The conflict will have at least the blocking data and the
		// conflicting properties.
		c := &merge.Conflict{
			Proposed: bags[sourceIdx],
			Target:   a.newBagLocked(),
		}

		// Copy the conflicting data from the table into the Conflict.
		for idx, col := range a.mu.templates.Columns {
			c.Target.Put(col.Name, blockingData[idx])
		}

		// Supply before data if we received it from upstream.
		conflictingMut := muts[sourceIdx]
		if len(conflictingMut.Before) > 0 {
			// Extra sanity-check for a literal null token.
			if !bytes.Equal(conflictingMut.Before, []byte("null")) {
				c.Before = a.newBagLocked()
				if err := c.Before.UnmarshalJSON(conflictingMut.Before); err != nil {
					return errors.WithStack(err)
				}
			}
		}

		conflicts = append(conflicts, c)
		conflictMuts = append(conflictMuts, conflictingMut)
	}
	// Final or no-rows error check.
	if err := conflictingRows.Err(); err != nil {
		return errors.WithStack(err)
	}

	a.upserts.Add(float64(len(bags)))
	log.WithFields(log.Fields{
		"conflicts": len(conflicts),
		"duration":  time.Since(start),
		"proposed":  len(bags),
		"target":    a.target,
		"upserted":  len(bags),
	}).Trace("conditionally upserted rows")

	// All rows were successfully applied.
	if len(conflicts) == 0 {
		return nil
	}
	a.conflicts.Add(float64(len(conflicts)))

	// Call the merge function on each conflict to generate a
	// replacement row, add to a DLQ, or drop entirely.
	fixups := make([]*merge.Bag, 0, len(conflicts))
	for idx, c := range conflicts {
		resolution, err := merger.Merge(ctx, c)
		switch {
		case err != nil:
			return err
		case resolution == nil:
			return errors.New("merge implementation returned nil *Resolution")
		case resolution.Drop:
			// No action needed.
		case resolution.DLQ != "":
			// Locate the requested DLQ and add the mutation.
			q, err := a.dlqs.Get(ctx, a.target.Base.Schema(), resolution.DLQ)
			if err != nil {
				return err
			}
			if err := q.Enqueue(ctx, db, conflictMuts[idx]); err != nil {
				return err
			}
		case resolution.Apply != nil:
			fixups = append(fixups, resolution.Apply)
		default:
			return errors.New("merge implementation returned zero-valued *Resolution")
		}
	}

	a.resolves.Add(float64(len(fixups)))
	log.WithField("resolves", len(fixups)).Trace("resolved merge conflicts")

	// We'll recurse into this function, but apply unconditionally.
	return a.upsertBagsLocked(ctx, db, applyUnconditional, nil, fixups, template)
}

// newBagLocked constructs a new property bag using cached metadata.
func (a *apply) newBagLocked() *merge.Bag {
	return merge.NewBag(a.mu.bagSpec)
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
	a.mu.bagSpec = &merge.BagSpec{
		Columns: tmpl.Columns,
		Rename:  tmpl.Renames,
	}
	a.mu.gen++
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
	if configData.Merger != nil {
		if len(configData.CASColumns)+configData.Deadlines.Len() == 0 {
			return errors.Errorf("a merge function is defined for %s, but no CAS or Deadline"+
				"columns are defined to trigger it", a.target)
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

// toColumns converts a linear slice of arguments into columnar values.
func toColumns(columns, rows int, data []any) ([]any, error) {
	if rows*columns != len(data) {
		return nil, errors.Errorf("expecting %d*%d elements, had %d", columns, rows, data)
	}

	// Each element is a slice of the column's values.
	colData := make([]reflect.Value, columns)
	ret := make([]any, columns)

	// We want to create typed column slices to hold each element.
	for colIdx := range columns {
		var eltType reflect.Type
		for rowIdx := range rows {
			if elt := data[rowIdx*columns+colIdx]; elt != nil {
				eltType = reflect.TypeOf(elt)
				break
			}
		}
		// If every element was nil, we'll send an array of null
		// strings.
		if eltType == nil {
			eltType = reflect.TypeFor[string]()
		}
		sliceType := reflect.SliceOf(eltType)
		rowData := reflect.MakeSlice(sliceType, rows, rows)
		colData[colIdx] = rowData
		ret[colIdx] = rowData.Interface()
	}

	for colIdx, colSlice := range colData {
		// The inner slice should have the same type as the element.
		for rowIdx := range rows {
			if elt := data[rowIdx*columns+colIdx]; elt != nil {
				colSlice.Index(rowIdx).Set(reflect.ValueOf(elt))
			}
		}
	}
	return ret, nil
}

// toDBType returns a replacement value if needed to pass the reified
// type to the database.
func toDBType(colData *types.ColData, value any) (any, error) {
	if num, ok := value.(json.Number); ok {
		// The JSON parser is configured to parse numbers as though
		// they were strings.  We'll keep the string encoding so
		// that the target database can tell us if the string value
		// exceeds the precision or scale for the target column.
		return removeExponent(num).String(), nil
	}
	if value != nil && colData.Parse != nil {
		// Target-driver specific fixups.
		next, err := colData.Parse(value)
		if err != nil {
			return nil, errors.Wrapf(err, "could not parse %v as a %s",
				value, colData.Type)

		}
		value = next
	}
	return value, nil
}

// toDBTypes updates the values slice in place to convert our reified
// value types into something the database is willing to accept.
func toDBTypes(colData []types.ColData, values []any) error {
	if len(colData) != len(values) {
		return errors.Errorf("length mismatch %d vs %d", len(colData), len(values))
	}
	for idx, value := range values {
		var err error
		values[idx], err = toDBType(&colData[idx], value)
		if err != nil {
			return err
		}
	}
	return nil
}

// IsMergeSupported returns true if the applier supports three-way
// merges for the given product.
//
// Work items in https://github.com/cockroachdb/cdc-sink/issues/487
func IsMergeSupported(product types.Product) bool {
	switch product {
	case types.ProductCockroachDB, types.ProductPostgreSQL:
		return true
	default:
		return false
	}
}
