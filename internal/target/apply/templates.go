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
		"isUDT": func(x any) bool {
			_, ok := x.(ident.UDT)
			return ok
		},
		// nl is a hack to force the inclusion of newlines in the output
		// since we generally use the whitespace-consuming template
		// syntax.
		"nl": func() string { return "\n" },

		// qualify is used with the "join" template to emit a list of
		// qualified database identifiers. The prefix is prepended to
		// each column's name using a dot separator.
		//   {{ range $name := qualify "foo" $.Columns }}
		// would return values such as
		//     foo.PK, foo.Val0, foo.Val1, ....
		"qualify": func(prefix any, cols []types.ColData) ([]string, error) {
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
	Columns    []types.ColData        // All non-ignored columns.
	Conditions []types.ColData        // The version-like fields for CAS ops.
	Deadlines  types.Deadlines        // Allow too-old data to just be dropped.
	Exprs      map[ident.Ident]string // Value-replacement expressions.
	PK         []types.ColData        // All primary-key columns.
	TableName  ident.Table            // The target table.
	cache      *templateCache         // Memoize calls to delete() and upsert().

	// The variables below here are updated during evaluation.

	RowCount int // The number of rows to be applied.
}

// newTemplates constructs a new templates instance, performing some
// pre-computations to identify primary keys and to filter out ignored
// columns.
func newTemplates(target ident.Table, cfgData *Config, colData []types.ColData) *templates {
	// Map cas column names to their order in the comparison tuple.
	casMap := make(map[ident.Ident]int, len(cfgData.CASColumns))
	for idx, name := range cfgData.CASColumns {
		casMap[name] = idx
	}

	ret := &templates{
		Conditions: make([]types.ColData, len(cfgData.CASColumns)),
		Columns:    append([]types.ColData(nil), colData...),
		Deadlines:  cfgData.Deadlines,
		Exprs:      cfgData.Exprs,
		TableName:  target,
		cache: &templateCache{
			deletes: lru.New(batches.Size()),
			upserts: lru.New(batches.Size()),
		},
	}

	// Filter out the ignored columns, build the list of PK columns,
	// and apply renames.
	// https://github.com/golang/go/wiki/SliceTricks#filter-in-place
	idx := 0
	for _, col := range ret.Columns {
		if col.Ignored || cfgData.Ignore[col.Name] {
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
	// If non-empty, a user-configured SQL expression for the value.
	// The Index substitution parameter will have been injected into
	// the expressed.
	Expr string
	// The 1-based SQL substitution parameter index. A zero value
	// indicates that the varPair has a constant expression which
	// does not depend on an injected value.
	Index int
}

// Vars is a generator function that returns windows of 1-based
// substitution parameters for the given columns. These are used to
// generate the multi-VALUES ($1,$2, ...), ($55, $56) clauses.
func (t *templates) Vars() [][]varPair {
	ret := make([][]varPair, t.RowCount)
	pairIdx := 1
	for row := range ret {
		ret[row] = make([]varPair, len(t.Columns))
		for colIdx, col := range t.Columns {
			vp := varPair{
				Column: col,
				Index:  pairIdx,
			}
			pairIdx++

			if pattern, ok := t.Exprs[col.Name]; ok {
				vp.Expr = strings.ReplaceAll(
					pattern, substitutionToken, fmt.Sprintf("$%d", vp.Index))
				// A constant expression doesn't occupy an index slot.
				if vp.Expr == pattern {
					vp.Index = 0
					pairIdx--
				}
			}

			ret[row][colIdx] = vp
		}
	}
	return ret
}

func (t *templates) delete(rowCount int) (string, error) {
	// Fast lookup
	t.cache.Lock()
	found, ok := t.cache.deletes.Get(rowCount)
	t.cache.Unlock()
	if ok {
		applyTemplateHits.WithLabelValues("delete").Inc()
		return found.(string), nil
	}
	applyTemplateMisses.WithLabelValues("delete").Inc()

	// Make a copy that we can tweak.
	cpy := *t
	cpy.Columns = t.PK
	cpy.RowCount = rowCount

	var buf strings.Builder
	err := deleteTemplate.Execute(&buf, &cpy)
	ret, err := buf.String(), errors.WithStack(err)
	if err == nil {
		t.cache.Lock()
		t.cache.deletes.Add(rowCount, ret)
		t.cache.Unlock()
	}
	return ret, err
}

func (t *templates) upsert(rowCount int) (string, error) {
	t.cache.Lock()
	found, ok := t.cache.upserts.Get(rowCount)
	t.cache.Unlock()
	if ok {
		applyTemplateHits.WithLabelValues("upsert").Inc()
		return found.(string), nil
	}
	applyTemplateMisses.WithLabelValues("upsert").Inc()

	// Make a copy that we can tweak.
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
		t.cache.Lock()
		t.cache.upserts.Add(rowCount, ret)
		t.cache.Unlock()
	}
	return ret, err
}
