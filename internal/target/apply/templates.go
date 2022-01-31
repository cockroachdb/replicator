// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package apply

import (
	"embed"
	"fmt"
	"strings"
	"sync"
	"text/template"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/batches"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/golang/groupcache/lru"
	"github.com/pkg/errors"
)

var (
	//go:embed queries/*.tmpl
	queries embed.FS

	parsed = template.Must(template.New("").Funcs(template.FuncMap{
		// nl is a hack to force the inclusion of newlines in the output
		// since we generally use the whitespace-consuming template
		// syntax.
		"nl": func() string { return "\n" },

		// qualify is used with the "join" template to emit a list
		// of qualified database identifiers.
		"qualify": func(prefix interface{}, cols []types.ColData) ([]string, error) {
			var id ident.Ident
			switch t := prefix.(type) {
			case string:
				id = ident.New(t)
			case ident.Ident:
				id = t
			case ident.Table:
				id = t.Table()
			default:
				return nil, errors.Errorf("unsupported conversion %T", t)
			}
			ret := make([]string, len(cols))
			for i := range ret {
				ret[i] = fmt.Sprintf("%s.%s", id, cols[i].Name)
			}
			return ret, nil
		},
	}).ParseFS(queries, "**/*.tmpl"))
	conditionalTemplate = parsed.Lookup("conditional.tmpl")
	deleteTemplate      = parsed.Lookup("delete.tmpl")
	upsertTemplate      = parsed.Lookup("upsert.tmpl")
)

// A templateCache stores variations of the delete and upsert commands
// at varying batch sizes. The map keys are just ints representing the
// number of rows that the statement should process.
type templateCache struct {
	sync.Mutex
	deletes *lru.Cache
	upserts *lru.Cache
}

type templates struct {
	Columns    []types.ColData // All non-ignored columns.
	Conditions []types.ColData // The version-like fields for CAS ops.
	Deadlines  types.Deadlines // Allow too-old data to just be dropped.
	PK         []types.ColData // All primary-key columns.
	TableName  ident.Table     // The target table.
	cache      *templateCache  // Memoize calls to delete() and upsert().

	// The variables below here are updated during evaluation.

	RowCount int // The number of rows to be applied.
}

// newTemplates constructs a new templates instance, performing some
// pre-computations to identify primary keys and to filter out ignored
// columns.
func newTemplates(
	target ident.Table, casNames []ident.Ident, deadlines types.Deadlines, colData []types.ColData,
) *templates {
	// Map cas column names to their order in the comparison tuple.
	casMap := make(map[ident.Ident]int, len(casNames))
	for idx, name := range casNames {
		casMap[name] = idx
	}

	ret := &templates{
		Conditions: make([]types.ColData, len(casNames)),
		Columns:    append([]types.ColData(nil), colData...),
		Deadlines:  deadlines,
		TableName:  target,
		cache: &templateCache{
			deletes: lru.New(batches.Size() / 10),
			upserts: lru.New(batches.Size() / 10),
		},
	}

	// Filter out the ignored columns and build the list of PK columns.
	// https://github.com/golang/go/wiki/SliceTricks#filter-in-place
	idx := 0
	for _, col := range ret.Columns {
		if col.Ignored {
			continue
		}
		ret.Columns[idx] = col
		idx++
		if col.Primary {
			ret.PK = append(ret.PK, col)
		}
		if idx, isCas := casMap[col.Name]; isCas {
			ret.Conditions[idx] = col
		}
	}
	ret.Columns = ret.Columns[:idx]
	return ret
}

// varPair is returned by Vars, to associate a Column with a
// substitution-parameter index.
type varPair struct {
	Column types.ColData
	Index  int
}

// Vars is a generator function that returns windows of 1-based
// substitution parameters for the given columns. These are used to
// generate the multi-VALUES ($1,$2, ...), ($55, $56) clauses.
func (t *templates) Vars() [][]varPair {
	ret := make([][]varPair, t.RowCount)
	for row := range ret {
		ret[row] = make([]varPair, len(t.Columns))
		for colIdx, col := range t.Columns {
			ret[row][colIdx] = varPair{
				Column: col,
				Index:  row*len(t.Columns) + colIdx + 1,
			}
		}
	}
	return ret
}

func (t *templates) delete(rowCount int) (string, error) {
	t.cache.Lock()
	defer t.cache.Unlock()
	if found, ok := t.cache.deletes.Get(rowCount); ok {
		return found.(string), nil
	}

	cpy := *t
	cpy.Columns = t.PK
	cpy.RowCount = rowCount

	var buf strings.Builder
	err := deleteTemplate.Execute(&buf, &cpy)
	ret, err := buf.String(), errors.WithStack(err)
	if err == nil {
		t.cache.deletes.Add(rowCount, ret)
	}
	return ret, err
}

func (t *templates) upsert(rowCount int) (string, error) {
	t.cache.Lock()
	defer t.cache.Unlock()
	if found, ok := t.cache.upserts.Get(rowCount); ok {
		return found.(string), nil
	}

	cpy := *t
	cpy.RowCount = rowCount

	var buf strings.Builder
	var err error
	if len(cpy.Conditions) == 0 && len(cpy.Deadlines) == 0 {
		err = upsertTemplate.Execute(&buf, &cpy)
	} else {
		err = conditionalTemplate.Execute(&buf, &cpy)
	}
	ret, err := buf.String(), errors.WithStack(err)
	if err == nil {
		t.cache.upserts.Add(rowCount, ret)
	}
	return ret, err
}
