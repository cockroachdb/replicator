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
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/applycfg"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	// QueryDir is the name of the subdirectory in the template [fs.FS].
	QueryDir = "queries"

	// TemplateOverrideEnv is the name of an environment variable that
	// overrides the [fs.FS] from which the query templates are loaded.
	// This facilitates ad-hoc debugging of queries, without the need to
	// recompile the cdc-sink binary.
	TemplateOverrideEnv = "CDC_SINK_TEMPLATES"
)

var (
	// EmbeddedTemplates bundles the templates into the binary.
	//go:embed queries/*/*.tmpl
	EmbeddedTemplates embed.FS

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

	tmplCRDB *template.Template
	tmplOra  *template.Template
	tmplMy   *template.Template
	tmplPG   *template.Template
)

// The error handling in this init function is panicky, since this
// feature is for debugging purposes only.
func init() {
	if err := loadTemplates(EmbeddedTemplates, os.Getenv(TemplateOverrideEnv)); err != nil {
		log.WithError(err).Fatal()
	}
}

// loadTemplates is called by init() and by test code.
func loadTemplates(from fs.FS, override string) error {
	// This error message can be specific since, in production, the only
	// way to get here is to set the environment variable.
	const msg = "the directory identified by the %s environment variable " +
		"must contain a %s subdirectory"

	if override != "" {
		path, err := filepath.Abs(override)
		if err != nil {
			return err
		}
		info, err := os.Stat(filepath.Join(path, "queries"))
		if err != nil {
			return errors.Wrapf(err, msg, TemplateOverrideEnv, QueryDir)
		}
		if !info.IsDir() {
			return errors.Errorf(msg, TemplateOverrideEnv, QueryDir)

		}
		from = os.DirFS(path)
	}

	load := func(dir string) (*template.Template, error) {
		path := filepath.Join(QueryDir, dir, "*.tmpl")
		ret, err := template.New("").Funcs(tmplFuncs).ParseFS(from, path)
		if err != nil {
			return nil, errors.Wrapf(err, "loading templates for %s", dir)
		}
		return ret, nil
	}
	var err error
	tmplCRDB, err = load("crdb")
	if err != nil {
		return err
	}
	tmplMy, err = load("my")
	if err != nil {
		return err
	}
	tmplOra, err = load("ora")
	if err != nil {
		return err
	}
	tmplPG, err = load("pg")
	return err
}

type templates struct {
	*columnMapping

	// If true, the templates generate SQL that expects the data to be
	// provided as N-many equal-length slices, where N is the number of
	// columns. The SQL is generated as though only a single row is
	// being operated on.
	BulkDelete bool
	BulkUpsert bool

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

	case types.ProductMySQL:
		ret.conditional = tmplMy.Lookup("conditional.tmpl")
		ret.delete = tmplMy.Lookup("delete.tmpl")
		ret.upsert = tmplMy.Lookup("upsert.tmpl")

	case types.ProductOracle:
		// Bulk execution of DELETE not supported. See:
		// github.com/sijms/go-ora/v2/command.go
		ret.BulkUpsert = true
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
	ret := make([][]varPair, t.RowCount)
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
				case types.ProductMySQL:
					reference = "?"
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
	if t.BulkDelete {
		rowCount = 1
	}

	// Make a copy that we can tweak.
	cpy := *t
	cpy.ForDelete = true
	cpy.RowCount = rowCount

	var buf strings.Builder
	err := t.delete.Execute(&buf, &cpy)
	return buf.String(), errors.WithStack(err)
}

// upsertExpr may return a conditional sql statement, unless forceUpsert
// is set to true.
func (t *templates) upsertExpr(rowCount int, mode applyMode) (string, error) {
	if t.BulkUpsert {
		rowCount = 1
	}

	// Make a copy that we can tweak.
	cpy := *t
	cpy.RowCount = rowCount

	var buf strings.Builder
	var err error
	if mode == applyUnconditional || len(cpy.Conditions) == 0 && cpy.Deadlines.Len() == 0 {
		err = t.upsert.Execute(&buf, &cpy)
	} else {
		err = t.conditional.Execute(&buf, &cpy)
	}
	return buf.String(), errors.WithStack(err)
}
