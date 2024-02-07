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

package stage

import (
	"embed"
	"strings"
	"text/template"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
)

//go:embed queries/*.tmpl
var templateFS embed.FS

var (
	tmplFuncs = template.FuncMap{
		"add": func(a, b int) int { return a + b },
		"nl":  func() string { return "\n" },
		"sp":  func() string { return " " },
	}
	templates = template.Must(
		template.New("").Funcs(tmplFuncs).ParseFS(templateFS, "queries/*.tmpl"))
	unstage = templates.Lookup("unstage.tmpl")
)

type templateData struct {
	Cursor         *types.UnstageCursor // Required input.
	IgnoreLeases   bool                 // Don't block leased keys.
	SetApplied     bool                 // Set the applied column to true.
	SetLeaseExpiry time.Time            // Set the lease column to this value.
	StagingSchema  ident.Schema         // Required input.
}

func newTemplateData(cursor *types.UnstageCursor, stagingSchema ident.Schema) *templateData {
	return &templateData{
		Cursor:         cursor,
		IgnoreLeases:   cursor.IgnoreLeases,
		SetApplied:     cursor.LeaseExpiry.IsZero(),
		SetLeaseExpiry: cursor.LeaseExpiry,
		StagingSchema:  stagingSchema,
	}
}

func (d *templateData) Eval() (string, error) {
	var sb strings.Builder
	err := unstage.Execute(&sb, d)
	if err != nil {
		return "", errors.WithStack(err)
	}
	return sb.String(), nil
}

func (d *templateData) StagingTable(tbl ident.Table) ident.Table {
	return stagingTable(d.StagingSchema, tbl)
}
