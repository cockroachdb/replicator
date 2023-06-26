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
	"sort"
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
	target ident.Table

	deletes   prometheus.Counter
	durations prometheus.Observer
	errors    prometheus.Counter
	upserts   prometheus.Counter

	mu struct {
		sync.RWMutex
		configData        *applycfg.Config
		expectedKeyLength int // Sanity-check "key" attribute
		schemaData        []types.ColData
		templates         *templates
	}
}

var _ types.Applier = (*apply)(nil)

// newApply constructs an apply by inspecting the target table.
func newApply(
	target ident.Table, cfgs *applycfg.Configs, watchers types.Watchers,
) (_ *apply, cancel func(), _ error) {
	labelValues := metrics.TableValues(target)
	a := &apply{
		target: target,

		deletes:   applyDeletes.WithLabelValues(labelValues...),
		durations: applyDurations.WithLabelValues(labelValues...),
		errors:    applyErrors.WithLabelValues(labelValues...),
		upserts:   applyUpserts.WithLabelValues(labelValues...),
	}

	// Start a background goroutine to refresh the templates.
	ctx, cancel := context.WithCancel(context.Background())
	errs := make(chan error, 1)
	go func(ctx context.Context, errs chan<- error) {
		defer cancel()

		w, err := watchers.Get(ctx, target.Database())
		if err != nil {
			errs <- err
			return
		}

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

	if len(a.mu.schemaData) == 0 {
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
	sql, err := a.mu.templates.delete(len(muts))
	if err != nil {
		return err
	}

	keyGroups := make([][]any, len(muts))
	if err := pjson.Decode(ctx, keyGroups, func(i int) []byte {
		return muts[i].Key
	}); err != nil {
		return err
	}

	allArgs := make([]any, 0, a.mu.expectedKeyLength*len(muts))
	for i, keyGroup := range keyGroups {
		if len(keyGroup) != a.mu.expectedKeyLength {
			return errors.Errorf(
				"schema drift detected in %s: "+
					"inconsistent number of key columns: "+
					"received %d expect %d: "+
					"key %s@%s",
				a.target,
				len(keyGroup), a.mu.expectedKeyLength,
				string(muts[i].Key), muts[i].Time)
		}
		allArgs = append(allArgs, keyGroup...)
	}

	for idx, arg := range allArgs {
		if num, ok := arg.(json.Number); ok {
			allArgs[idx] = removeExponent(num)
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

	sql, err := a.mu.templates.upsert(len(muts))
	if err != nil {
		return err
	}

	// Allocate a slice for all mutation data. We'll reset the length
	// once we know how many elements we actually have.
	allArgs := make([]any, len(a.mu.schemaData)*len(muts))
	argIdx := 0
	// We'll remember the current location for any extra arguments
	// that we see, so we can backtrack to fill in the blank.
	extrasArgIdx := -1

	// Decode the mutations into an actionable map.
	columnData := make([]map[ident.Ident]any, len(muts))
	if err := pjson.Decode(ctx, columnData,
		func(i int) []byte { return muts[i].Data },
	); err != nil {
		return err
	}

	for i, incomingColumnData := range columnData {
		// Track the columns that we expect to see and that are seen in
		// the incoming payload. This improves the error returned when
		// there are unexpected columns.
		knownColumnsInPayload := make(map[ident.Ident]struct{}, len(a.mu.schemaData))

		for _, col := range a.mu.schemaData {
			// Determine which key to look for in the mutation payload.
			// If there's no explicit configuration, use the target
			// column's name.
			sourceCol, renamed := a.mu.configData.SourceNames[col.Name]
			if !renamed {
				sourceCol = col.Name
			}
			decoded, presentInPayload := incomingColumnData[sourceCol]
			// Keep track of columns in the incoming payload that match
			// columns that we expect to see in the target database.
			if presentInPayload {
				knownColumnsInPayload[sourceCol] = struct{}{}
			}
			// Ignored will be true for columns in the target database
			// that we know about, but that we don't actually want to
			// insert new values for (e.g. computed columns). These
			// ignored columns could be part of the primary key, or they
			// could be a regular column. We also allow the user to
			// force columns to be ignored (e.g. to drop a column).
			if col.Ignored || a.mu.configData.Ignore[col.Name] {
				continue
			}
			// We allow the user to specify an arbitrary expression for
			// a column value. If there's no $0 substitution token, then
			// we want to drop the column from the values to be sent
			// with the query. The templates will bake in the fixed
			// expression.
			if expr, ok := a.mu.configData.Exprs[col.Name]; ok {
				if !strings.Contains(expr, applycfg.SubstitutionToken) {
					continue
				}
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
					a.target, sourceCol.Raw(),
					string(muts[i].Key), muts[i].Time)
			}

			if col.Name == a.mu.configData.Extras {
				extrasArgIdx = argIdx
			}
			allArgs[argIdx] = decoded
			argIdx++
		}

		// Pretend as though we've seen any ignored columns.
		for col := range a.mu.configData.Ignore {
			knownColumnsInPayload[col] = struct{}{}
		}

		// Collect unknown / unmapped columns into the extras blob,
		// or error out if we have no place to store extras.
		if extraCount := len(incomingColumnData) - len(knownColumnsInPayload); extraCount > 0 {
			if a.mu.configData.Extras.IsEmpty() {
				var unmapped []string
				for key := range incomingColumnData {
					if _, seen := knownColumnsInPayload[key]; !seen {
						unmapped = append(unmapped, key.Raw())
					}
				}
				sort.Strings(unmapped)
				return errors.Errorf(
					"schema drift detected in %s: "+
						"unexpected columns %v: "+
						"key %s@%s",
					a.target, unmapped, string(muts[i].Key), muts[i].Time)
			}

			unmapped := make(map[ident.Ident]any, extraCount)
			for key, value := range incomingColumnData {
				if _, seen := knownColumnsInPayload[key]; !seen {
					unmapped[key] = value
				}
			}
			// Find the location in the args slice to update
			// with the extra data.
			if extrasArgIdx < 0 {
				return errors.Errorf(
					"extras column %s not found the target schema",
					a.mu.configData.Extras)
			}
			allArgs[extrasArgIdx] = unmapped
		}
	}

	// Done accumulating data, trim the slice.
	allArgs = allArgs[:argIdx]

	for idx, arg := range allArgs {
		if num, ok := arg.(json.Number); ok {
			allArgs[idx] = removeExponent(num)
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
	// We want to verify that the cas and deadline columns actually
	// exist in the incoming column data.
	allColNames := make(map[ident.Ident]struct{}, len(schemaData))
	for _, col := range schemaData {
		allColNames[col.Name] = struct{}{}
	}
	for _, col := range configData.CASColumns {
		if _, found := allColNames[col]; !found {
			return errors.Errorf("cas column name %s not found in table %s", col, a.target)
		}
	}
	for col := range configData.Deadlines {
		if _, found := allColNames[col]; !found {
			return errors.Errorf("deadline column name %s not found in table %s", col, a.target)
		}
	}
	for col := range configData.Exprs {
		if _, found := allColNames[col]; !found {
			return errors.Errorf("expression column name %s not found in table %s", col, a.target)
		}
	}

	// The Ignores field doesn't need validation, since you might want
	// to mark a column as ignored in order to (eventually) drop it from
	// the destination database.

	for col := range configData.SourceNames {
		if _, found := allColNames[col]; !found {
			return errors.Errorf("renamed column name %s not found in table %s", col, a.target)
		}
	}

	// Compute the expected length of the "key" attribute in the
	// incoming changefeed messages. This length may be greater than the
	// number of PKs that we'll actually write to, since columns with a
	// generation expression are included in the changefeed.
	var expectedKeyLength int
	for _, col := range schemaData {
		// PKs are always first in the slice.
		if !col.Primary {
			break
		}
		// Special case: If the primary key is hash-sharded, the
		// changefeed strips the shard from the key before sending it.
		if strings.HasPrefix(col.Name.Raw(), "crdb_internal_") {
			continue
		}
		// Note that we include any other "ignored" columns to support
		// schemas with PKs that contain generated columns (e.g. custom
		// sharding approaches).
		expectedKeyLength++
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	a.mu.configData = configData
	a.mu.schemaData = schemaData
	a.mu.expectedKeyLength = expectedKeyLength
	a.mu.templates = newTemplates(a.target, configData, schemaData)
	return nil
}
