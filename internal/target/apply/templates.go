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
	"text/template"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
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

		"oneBased": func(idx int) int { return idx + 1 },

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

type templates struct {
	Columns    []types.ColData // All non-ignored columns.
	Conditions []types.ColData // The version-like fields for CAS ops.
	Deadlines  types.Deadlines // Allow too-old data to just be dropped.
	PK         []types.ColData // All primary-key columns.
	TableName  ident.Table     // The target table.
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

func (t *templates) delete() (string, error) {
	var buf strings.Builder
	err := deleteTemplate.Execute(&buf, t)
	return buf.String(), errors.WithStack(err)
}

func (t *templates) upsert() (string, error) {
	var buf strings.Builder
	var err error
	if len(t.Conditions) == 0 && len(t.Deadlines) == 0 {
		err = upsertTemplate.Execute(&buf, t)
	} else {
		err = conditionalTemplate.Execute(&buf, t)
	}
	return buf.String(), errors.WithStack(err)
}
