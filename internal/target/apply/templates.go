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
		"oneBased": func(idx int) int { return idx + 1 },
	}).ParseFS(queries, "**/*.tmpl"))
	deleteTemplate = parsed.Lookup("delete.tmpl")
	upsertTemplate = parsed.Lookup("upsert.tmpl")
)

type templates struct {
	Columns   []types.ColData // All non-ignored columns.
	PK        []types.ColData // All primary-key columns.
	TableName ident.Table     // The target table.
}

// newTemplates constructs a new templates instance, performing some
// pre-computations to identify primary keys and to filter out ignored
// columns.
func newTemplates(target ident.Table, colData []types.ColData) *templates {
	ret := &templates{
		Columns:   colData,
		TableName: target,
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
	err := upsertTemplate.Execute(&buf, t)
	return buf.String(), errors.WithStack(err)
}
