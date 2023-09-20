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

package apply

import (
	"embed"
	"fmt"
	"strings"
	"text/template"

	"github.com/cockroachdb/cdc-sink/internal/staging/applycfg"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
)

var (
	//go:embed queries/*/*.tmpl
	queries embed.FS

	tmplFuncs = template.FuncMap{
		// The pgx driver has trouble converting from the []any it gets
		// when the mutation payload is unpacked, since it doesn't
		// necessarily know the type details of an enum array. This hack
		// changes the generated SQL so that the substitution parameter
		// type is a string array, which is then cast to the enum array.
		"isUDTArray": func(colData types.ColData) bool {
			return strings.HasSuffix(colData.Type, "[]") && strings.Contains(colData.Type, ".")
		},
		// nl is a hack to force the inclusion of newlines in the output
		// since we generally use the whitespace-consuming template
		// syntax.
		"nl": func() string { return "\n" },

		// sp just inserts a space into the output. This is useful when
		// using whitespace-consuming template markers.
		"sp": func() string { return " " },

		// qualify is used with the "join" template to emit a list of
		// qualified database identifiers. The prefix is prepended to
		// each column's name using a dot separator.
		//   {{ range $name := qualify "foo" $.Columns }}
		// would return values such as
		//     foo.PK, foo.Val0, foo.Val1, ....
		"qualify": func(prefix any, cols []types.ColData) ([]string, error) {
			var id string
			switch t := prefix.(type) {
			case string:
				id = t
			case ident.Ident:
				id = t.String()
			case ident.Table:
				id = t.Table().String()
			default:
				return nil, errors.Errorf("unsupported conversion %T", t)
			}
			ret := make([]string, len(cols))
			for i := range ret {
				ret[i] = fmt.Sprintf("%s.%s", id, cols[i].Name)
			}
			return ret, nil
		},
	}

	tmplCRDB = template.Must(template.New("").Funcs(tmplFuncs).ParseFS(queries, "queries/crdb/*.tmpl"))
	tmplOra  = template.Must(template.New("").Funcs(tmplFuncs).ParseFS(queries, "queries/ora/*.tmpl"))
	tmplPG   = template.Must(template.New("").Funcs(tmplFuncs).ParseFS(queries, "queries/pg/*.tmpl"))
)

type templates struct {
	*columnMapping

	// If true, the templates generate SQL that expects the data to be
	// provided as N-many equal-length slices, where N is the number of
	// columns.
	ColumnarDelete bool
	ColumnarUpsert bool

	conditional *template.Template
	delete      *template.Template
	upsert      *template.Template

	// The variables below here are updated during evaluation.
	ForDelete bool // True if we only iterate over PKs to delete
	RowCount  int  // The number of rows to be applied.
}

// newTemplates constructs a new templates instance, performing some
// pre-computations to identify primary keys and to filter out ignored
// columns.
func newTemplates(mapping *columnMapping) (*templates, error) {
	ret := &templates{columnMapping: mapping}

	switch mapping.Product {
	case types.ProductCockroachDB:
		ret.conditional = tmplCRDB.Lookup("conditional.tmpl")
		ret.delete = tmplCRDB.Lookup("delete.tmpl")
		ret.upsert = tmplCRDB.Lookup("upsert.tmpl")

	case types.ProductOracle:
		// Bulk execution of DELETE not supported.
		ret.ColumnarUpsert = true
		ret.delete = tmplOra.Lookup("delete.tmpl")
		ret.upsert = tmplOra.Lookup("upsert.tmpl")
		ret.conditional = ret.upsert

	case types.ProductPostgreSQL:
		ret.conditional = tmplPG.Lookup("conditional.tmpl")
		ret.delete = tmplPG.Lookup("delete.tmpl")
		ret.upsert = tmplPG.Lookup("upsert.tmpl")

	default:
		return nil, errors.Errorf("unsupported product %s", mapping.Product)
	}

	return ret, nil
}

// varPair is returned by Vars, to associate a Column with a
// substitution-parameter index.
type varPair struct {
	Column types.ColData
	// If non-empty, a user-configured SQL expression for the value
	// which overrides any other SQL generation that we may perform. The
	// Index substitution parameter will have been injected into the
	// expression.
	Expr string
	// A 1-based SQL substitution parameter index. A zero value
	// indicates that the varPair has a constant expression which
	// does not depend on an injected value.
	Param int
	// A 1-based parameter for a boolean value to indicate if a value
	// for the column was present in the original payload. A zero
	// value means the template should always expect a value.
	ValidityParam int
}

// Vars is a generator function that returns windows of 1-based
// substitution parameters for the given columns. These are used to
// generate the multi-VALUES ($1,$2, ...), ($55, $56) clauses.
func (t *templates) Vars() ([][]varPair, error) {
	var ret [][]varPair
	if t.ForDelete && t.ColumnarDelete || !t.ForDelete && t.ColumnarUpsert {
		ret = make([][]varPair, 1)
	} else {
		ret = make([][]varPair, t.RowCount)
	}
	cols := t.Columns
	if t.ForDelete {
		cols = t.PKDelete
	}

	for row := range ret {
		ret[row] = make([]varPair, len(cols))
		for colIdx, col := range cols {
			vp := varPair{Column: col}

			positions := t.Positions.GetZero(col.Name)
			if t.ForDelete {
				offset := row * t.DeleteParameterCount
				if positions.DeleteIndex >= 0 {
					vp.Param = offset + positions.DeleteIndex + 1
				} else {
					return nil, errors.Errorf("no delete position for %s", col.Name)
				}
			} else {
				offset := row * t.UpsertParameterCount
				vp.Param = offset + positions.UpsertIndex + 1

				// ValidityIndex is optional.
				if positions.ValidityIndex >= 0 {
					vp.ValidityParam = offset + positions.ValidityIndex + 1
				}
			}

			if pattern, ok := t.Exprs.Get(col.Name); ok {
				var reference string
				switch t.Product {
				case types.ProductCockroachDB, types.ProductPostgreSQL:
					reference = fmt.Sprintf("$%d", vp.Param)
				case types.ProductOracle:
					reference = fmt.Sprintf(":ref%d", vp.Param)
				default:
					return nil, errors.Errorf("unimplemented product %s", t.Product)
				}

				vp.Expr = strings.ReplaceAll(pattern, applycfg.SubstitutionToken, reference)
			}

			ret[row][colIdx] = vp
		}
	}
	return ret, nil
}

func (t *templates) deleteExpr(rowCount int) (string, error) {
	// Make a copy that we can tweak.
	cpy := *t
	cpy.ForDelete = true
	cpy.RowCount = rowCount

	var buf strings.Builder
	err := t.delete.Execute(&buf, &cpy)
	return buf.String(), errors.WithStack(err)
}

func (t *templates) upsertExpr(rowCount int) (string, error) {
	// Make a copy that we can tweak.
	cpy := *t
	cpy.RowCount = rowCount

	var buf strings.Builder
	var err error
	if len(cpy.Conditions) == 0 && cpy.Deadlines.Len() == 0 {
		err = t.upsert.Execute(&buf, &cpy)
	} else {
		err = t.conditional.Execute(&buf, &cpy)
	}
	return buf.String(), errors.WithStack(err)
}
