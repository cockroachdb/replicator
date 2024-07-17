// Copyright 2024 The Cockroach Authors
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

// Package load allows sparse records to be loaded from target tables.
package load

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/crep"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/merge"
	"github.com/cockroachdb/replicator/internal/util/metrics"
	"github.com/pkg/errors"
)

//go:embed queries/*.tmpl
var templateFS embed.FS

// This type is also used by the templates.
type demand struct {
	Bags          []*merge.Bag        // The containers to populate.
	PKs           []*types.ColData    // The primary key columns to query.
	PKData        [][]any             // ( Row x Col ) PK values.
	Product       types.Product       // Target database info.
	SelectCols    []*types.ColData    // The columns to load from the target.
	SelectTargets [][]*merge.Entry    // ( Row x Col ) Destinations for loaded values.
	Table         ident.Table         // Table to be queried.
	TX            types.TargetQuerier // Access to the database.
}

// Key returns a statement-cache lookup key.
func (d *demand) Key() string {
	var sb strings.Builder
	sb.WriteString(strconv.Itoa(len(d.Bags)))
	sb.WriteRune(':')
	sb.WriteString(d.Table.Raw())
	for _, col := range d.SelectCols {
		sb.WriteRune(':')
		sb.WriteString(col.Name.Raw())
	}
	return sb.String()
}

// Result is returned by [Loader.Load].
type Result struct {
	Dirty      []*merge.Bag // Property bags whose entries were populated.
	NotFound   []*merge.Bag // Property bags that are not present in the database.
	Unmodified []*merge.Bag // Property bags that were already valid.
}

// A Loader accesses a target table to populate missing values within a
// [merge.Bag].
type Loader struct {
	pool           *types.TargetPool
	selectTemplate *template.Template
	statements     *types.TargetStatements
}

// Load will examine the property bags for missing column values and
// populate them from the target table.
//
// This method updates the property
// bags in place, however it will return a slice containing the
// instances that were modified, if additional processing is necessary.
//
// The values that are placed into the property bags will be processed
// by [crep.Canonical] to ensure reasonably consistent behavior.
func (l *Loader) Load(
	ctx context.Context, tx types.TargetQuerier, table ident.Table, bags []*merge.Bag,
) (*Result, error) {
	start := time.Now()
	demands := make(map[string]*demand)
	res := &Result{
		Dirty:      make([]*merge.Bag, 0, len(bags)),
		NotFound:   make([]*merge.Bag, 0, len(bags)),
		Unmodified: make([]*merge.Bag, 0, len(bags)),
	}

	for _, bag := range bags {
		// Most or all of the incoming bags will be missing the same set
		// of properties. We want to aggregate the demands together
		// based on the set of missing keys. We would expect that, in
		// the case of changefeeds that involve column families, there
		// will be only a handful of distinct missing-column patterns to
		// load.
		var demandKey string
		var pkData []any
		var selectTargets []*merge.Entry

		for _, col := range bag.Columns {
			// Require PK values for all bags that we want to load.
			if col.Primary {
				pkEntry, ok := bag.Mapped.Get(col.Name)
				if !ok {
					return nil, errors.Errorf("missing PK column: %s", col.Name)
				}
				pkData = append(pkData, pkEntry.Value)
				continue
			}
			// Skip already-valid columns.
			entry, ok := bag.Entry(col.Name)
			if !ok {
				return nil, errors.Errorf("missing Entry for column: %s", col.Name)
			}
			if entry.Valid {
				continue
			}

			// Aggregate bags with similar patterns together.
			demandKey = fmt.Sprintf("%s:%s", demandKey, col.Name)
			selectTargets = append(selectTargets, entry)
		}

		// All properties are loaded, so do nothing.
		if len(selectTargets) == 0 {
			res.Unmodified = append(res.Unmodified, bag)
			continue
		}

		// Append request to the existing demand.
		if work, ok := demands[demandKey]; ok {
			work.Bags = append(work.Bags, bag)
			work.PKData = append(work.PKData, pkData)
			work.SelectTargets = append(work.SelectTargets, selectTargets)
			continue
		}

		// Otherwise, populate a new demand request.
		work := &demand{
			Bags:          []*merge.Bag{bag},
			PKData:        [][]any{pkData},
			Product:       l.pool.Product,
			SelectTargets: [][]*merge.Entry{selectTargets},
			Table:         table,
			TX:            tx,
		}
		demands[demandKey] = work

		for _, col := range bag.Columns {
			if !col.Primary {
				break
			}
			work.PKs = append(work.PKs, &col)
		}
		for _, tgt := range selectTargets {
			work.SelectCols = append(work.SelectCols, tgt.Column)
		}
	}

	labels := metrics.TableValues(table)

	// Load the missing data.
	for _, demand := range demands {
		qStart := time.Now()
		found, err := l.load(ctx, demand)
		if err != nil {
			return nil, err
		}

		for _, bag := range demand.Bags {
			if _, didFind := found[bag]; didFind {
				res.Dirty = append(res.Dirty, bag)
			} else {
				res.NotFound = append(res.NotFound, bag)
			}
		}
		loadQueryDurations.WithLabelValues(labels...).Observe(time.Since(qStart).Seconds())
	}

	loadOverallDurations.WithLabelValues(labels...).Observe(time.Since(start).Seconds())
	loadDirty.WithLabelValues(labels...).Add(float64(len(res.Dirty)))
	loadNotFound.WithLabelValues(labels...).Add(float64(len(res.NotFound)))
	loadUnmodified.WithLabelValues(labels...).Add(float64(len(res.Unmodified)))

	return res, nil
}

func (l *Loader) load(ctx context.Context, work *demand) (map[*merge.Bag]struct{}, error) {
	args := make([]any, len(work.PKs)*len(work.PKData))
	argIdx := 0
	for _, pksForRow := range work.PKData {
		for pkColIdx, pkColValue := range pksForRow {
			// Perform any necessary database type fixups.
			if fn := work.PKs[pkColIdx].Parse; fn != nil {
				var err error
				pkColValue, err = fn(pkColValue)
				if err != nil {
					return nil, err
				}
			}
			args[argIdx] = pkColValue
			argIdx++
		}
	}

	var err error
	var rows *sql.Rows
	if l.pool.Product == types.ProductMariaDB {
		// This works around a prepared-statement parameter type binding
		// difference between MariaDB and MySQL. We see the join against
		// the CTE data block fail to match any rows in the target
		// table. We rely on the driver having been configured to
		// textually interpolate arguments before executing a query.
		var buf strings.Builder
		if err := l.selectTemplate.Execute(&buf, work); err != nil {
			return nil, err
		}
		rows, err = work.TX.QueryContext(ctx, buf.String(), args...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	} else {
		// For all other databases, we'll rely on a cached statement.
		stmt, err := l.statements.Prepare(ctx, work.TX, work.Key(), func() (string, error) {
			var buf strings.Builder
			if err := l.selectTemplate.Execute(&buf, work); err != nil {
				return "", err
			}
			return buf.String(), nil
		})
		if err != nil {
			return nil, err
		}

		rows, err = stmt.QueryContext(ctx, args...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}

	found := make(map[*merge.Bag]struct{}, len(work.PKData))
	for rows.Next() {
		var targetIdx int
		values := make([]any, len(work.SelectCols))
		destPtrs := make([]any, len(values)+1)
		destPtrs[0] = &targetIdx
		for i := range values {
			destPtrs[i+1] = &values[i]
		}
		if err := rows.Scan(destPtrs...); err != nil {
			return nil, errors.WithStack(err)
		}
		found[work.Bags[targetIdx]] = struct{}{}
		for selectIdx, value := range values {
			entry := work.SelectTargets[targetIdx][selectIdx]
			entry.Value, err = crep.Canonical(value)
			if err != nil {
				return nil, errors.Wrapf(err, "targetIdx %d col %d", targetIdx, selectIdx)
			}
			entry.Valid = true
		}
	}
	if err := rows.Err(); err != nil {
		return nil, errors.WithStack(err)
	}
	return found, nil
}
