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
	"sync"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/applycfg"
	"github.com/cockroachdb/cdc-sink/internal/util/batches"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/merge"
	"github.com/cockroachdb/cdc-sink/internal/util/metrics"
	"github.com/cockroachdb/cdc-sink/internal/util/msort"
	"github.com/cockroachdb/cdc-sink/internal/util/pjson"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

// apply will upsert mutations and deletions into a target table.
type apply struct {
	cache   *types.TargetStatements
	product types.Product
	target  ident.Table

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

var _ types.Applier = (*apply)(nil)

type applyMode bool

const (
	applyConditional   applyMode = false // Use conditional apply and enable merge, if defined.
	applyUnconditional applyMode = true  // Don't use CAS support, just apply the data.
)

// newApply constructs an apply by inspecting the target table.
func newApply(
	cache *types.TargetStatements,
	product types.Product,
	inTarget ident.Table,
	cfgs *applycfg.Configs,
	watchers types.Watchers,
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
		cache:   cache,
		product: product,
		target:  target,

		conflicts: applyConflicts.WithLabelValues(labelValues...),
		deletes:   applyDeletes.WithLabelValues(labelValues...),
		durations: applyDurations.WithLabelValues(labelValues...),
		errors:    applyErrors.WithLabelValues(labelValues...),
		resolves:  applyResolves.WithLabelValues(labelValues...),
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

		configHandle := cfgs.Get(target)
		configData, configChanged := configHandle.Get()

		if configData.Merger != nil {
			switch a.product {
			case types.ProductCockroachDB:
			case types.ProductPostgreSQL:
			default:
				// Work items in https://github.com/cockroachdb/cdc-sink/issues/487
				errs <- errors.Errorf("merge operation not implemented for %s", a.product)
				return
			}
		}

		var schemaData []types.ColData
		for {
			select {
			case <-ctx.Done():
				return
			case <-configChanged:
				configData, configChanged = configHandle.Get()
			case schemaData = <-schemaCh:
			}
			if len(schemaData) > 0 {
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

	// Final flush.
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

	stmt, err := a.cache.Prepare(ctx,
		db,
		fmt.Sprintf("delete-%s-%d-%d", a.target, a.mu.gen, len(muts)),
		func() (string, error) {
			return a.mu.templates.deleteExpr(len(muts))
		})
	if err != nil {
		return err
	}

	if a.mu.templates.BulkDelete {
		allArgs, err = toColumns(len(a.mu.templates.PKDelete), len(muts), allArgs)
		if err != nil {
			return err
		}
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
	ctx context.Context, db types.TargetQuerier, muts []types.Mutation,
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

	return a.upsertBagsLocked(ctx, db, applyConditional, muts, allPayloadData)
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
			value := entry.Value
			if num, ok := value.(json.Number); ok {
				// The JSON parser is configured to parse numbers as though
				// they were strings.  We'll keep the string encoding so
				// that the target database can tell us if the string value
				// exceeds the precision or scale for the target column.
				value = removeExponent(num).String()
			} else if value != nil && entry.Column.Parse != nil {
				// Target-driver specific fixups.
				v, err := entry.Column.Parse(value)
				if err != nil {
					return errors.Wrapf(err, "could not parse %v as a %s",
						value, entry.Column.Type)
				}
				value = v
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
				allArgs[argIdx+targetColumn.ValidityIndex] = true
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
	// transaction.
	stmt, err := a.cache.Prepare(ctx,
		db,
		fmt.Sprintf("upsert-%s-%d-%d-%v", a.target, a.mu.gen, len(bags), mode),
		func() (string, error) {
			return a.mu.templates.upsertExpr(len(bags), mode)
		})
	if err != nil {
		return err
	}

	merger := a.mu.templates.Merger

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
		if conflictingMut := muts[sourceIdx]; len(conflictingMut.Before) > 0 {
			// Extra sanity-check for a literal null token.
			if !bytes.Equal(conflictingMut.Before, []byte("null")) {
				c.Before = a.newBagLocked()
				if err := c.Before.UnmarshalJSON(conflictingMut.Before); err != nil {
					return errors.WithStack(err)
				}
			}
		}

		conflicts = append(conflicts, c)
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
	for _, c := range conflicts {
		resolution, err := merger.Merge(ctx, c)
		switch {
		case err != nil:
			return err
		case resolution == nil:
			return errors.New("merge implementation returned nil *Resolution")
		case resolution.Drop:
			continue
		case resolution.DLQ != "":
			// Work item in https://github.com/cockroachdb/cdc-sink/issues/487
			return errors.New("unimplemented: DLQs are not implemented yet")
		case resolution.Apply != nil:
			fixups = append(fixups, resolution.Apply)
		default:
			return errors.New("merge implementation returned zero-valued *Resolution")
		}
	}

	a.resolves.Add(float64(len(fixups)))
	log.WithField("resolves", len(fixups)).Trace("resolved merge conflicts")

	// We'll recurse into this function, but apply unconditionally.
	return a.upsertBagsLocked(ctx, db, applyUnconditional, nil, fixups)
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
	colData := make([]any, columns)
	for colIdx := range colData {
		rowData := make([]any, rows)
		colData[colIdx] = rowData
		for rowIdx := range rowData {
			rowData[rowIdx] = data[rowIdx*columns+colIdx]
		}
	}
	return colData, nil
}
