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

package load

import (
	"text/template"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/google/wire"
)

// Set is used by Wire.
var Set = wire.NewSet(ProvideLoader)

// ProvideLoader is called by Wire.
func ProvideLoader(statements *types.TargetStatements, target *types.TargetPool) (*Loader, error) {
	templates, err := template.New("").Funcs(template.FuncMap{
		"add":       func(a, b int) int { return a + b },
		"inc":       func(i int) int { return i + 1 },
		"isMariaDB": func() bool { return target.Product == types.ProductMariaDB },
		"nl":        func() string { return "\n" },
		"sp":        func() string { return " " },
	}).ParseFS(templateFS, "queries/*.tmpl")
	if err != nil {
		return nil, err
	}

	l := &Loader{statements: statements, pool: target}
	switch target.Product {
	case types.ProductCockroachDB, types.ProductPostgreSQL:
		l.selectTemplate = templates.Lookup("pg.tmpl")
	case types.ProductMySQL:
		// TODO(bob): This SQL doesn't work with MariaDB; there appears
		// to be some kind of type coercion or parameter-encoding issue
		// where the JOIN between the key CTE data and the base table
		// results in no rows being matched. If this use-case must be
		// supported, we could look at using a much simpler select
		// statement that only matches a single row by PK.
		l.selectTemplate = templates.Lookup("my.tmpl")
	case types.ProductOracle:
		l.selectTemplate = templates.Lookup("ora.tmpl")
	default:
		// Loader.Load will return an error if the template is nil and
		// there are sparse entries.
	}
	return l, nil
}
