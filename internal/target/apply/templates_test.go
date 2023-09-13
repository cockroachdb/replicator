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
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/staging/applycfg"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/stretchr/testify/require"
)

// Set this to true to rewrite the golden-output files.
const rewriteFiles = false

// Ensure that we can't merge this test if rewrite is true.
func TestRewriteShouldBeFalse(t *testing.T) {
	require.False(t, rewriteFiles)
}

func TestQueryTemplatesOra(t *testing.T) {
	global := &templateGlobal{
		cols: []types.ColData{
			{
				Name:    ident.New("pk0"),
				Primary: true,
				Type:    "VARCHAR(256)",
			},
			{
				Name:    ident.New("pk1"),
				Primary: true,
				Type:    "INT",
			},
			{
				Name: ident.New("val0"),
				Type: "VARCHAR(256)",
			},
			{
				Name: ident.New("val1"),
				Type: "VARCHAR(256)",
			},
			{
				Ignored: true,
				Name:    ident.New("ignored_pk"),
				Primary: true,
				Type:    "INT",
			},
			{
				Ignored: true,
				Name:    ident.New("ignored_val"),
				Primary: false,
				Type:    "INT",
			},
			{
				Name:        ident.New("has_default"),
				Type:        "INT8",
				DefaultExpr: "expr()",
			},
		},
		dir:     "ora",
		product: types.ProductOracle,
		tableID: ident.NewTable(
			ident.MustSchema(ident.New("schema")),
			ident.New("table")),
	}

	tcs := []*templateTestCase{
		{
			name: "base",
		},
		{
			name: "cas",
			cfg: &applycfg.Config{
				CASColumns: []ident.Ident{ident.New("val1"), ident.New("val0")},
			},
		},
		{
			name: "deadline",
			cfg: &applycfg.Config{
				Deadlines: ident.MapOf[time.Duration](
					ident.New("val1"), time.Second,
					ident.New("val0"), time.Hour,
				),
			},
		},
		{
			name: "casDeadline",
			cfg: &applycfg.Config{
				CASColumns: []ident.Ident{ident.New("val1"), ident.New("val0")},
				Deadlines: ident.MapOf[time.Duration](
					ident.New("val0"), time.Hour,
					ident.New("val1"), time.Second,
				),
			},
		},
		{
			// This ignore setup results in a PK-only table.
			name: "ignore",
			cfg: &applycfg.Config{
				Ignore: ident.MapOf[bool](
					"val0", true,
					"val1", true,
				)},
		},
		{
			// Changing the source names should have no effect on the
			// SQL that gets generated; we only care about the different
			// value when looking up values in the incoming mutation.
			name: "source names",
			cfg: &applycfg.Config{
				SourceNames: ident.MapOf[applycfg.SourceColumn](
					ident.New("val1"), ident.New("val1Renamed"),
					ident.New("unknown"), ident.New("is ok"),
				),
			},
		},
		{
			// Verify user-configured expressions, with zero, one, and
			// multiple uses of the substitution position.
			name: "expr",
			cfg: &applycfg.Config{
				Exprs: ident.MapOf[string](
					ident.New("val0"), `'fixed'`, // Doesn't consume a parameter slot.
					ident.New("val1"), `$0||'foobar'`,
					ident.New("pk1"), `$0+$0`,
				),
				Ignore: ident.MapOf[bool](
					ident.New("geom"), true,
					ident.New("geog"), true,
				),
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			checkTemplate(t, global, tc)
		})
	}
}

func TestQueryTemplatesPG(t *testing.T) {
	t.Run("crdb", func(t *testing.T) { testQueryTemplates(t, types.ProductCockroachDB) })
	t.Run("pg", func(t *testing.T) { testQueryTemplates(t, types.ProductPostgreSQL) })
}

func testQueryTemplates(t *testing.T, product types.Product) {
	t.Helper()
	global := &templateGlobal{
		cols: []types.ColData{
			{
				Name:    ident.New("pk0"),
				Primary: true,
				Type:    "STRING",
			},
			{
				Name:    ident.New("pk1"),
				Primary: true,
				Type:    "INT8",
			},
			{
				Name: ident.New("val0"),
				Type: "STRING",
			},
			{
				Name: ident.New("val1"),
				Type: "STRING",
			},
			{
				Ignored: true,
				Name:    ident.New("ignored_pk"),
				Primary: true,
				Type:    "STRING",
			},
			{
				Ignored: true,
				Name:    ident.New("ignored_val"),
				Primary: false,
				Type:    "STRING",
			},
			// Field types with special handling
			{
				Name: ident.New("geom"),
				Type: "GEOMETRY",
			},
			{
				Name: ident.New("geog"),
				Type: "GEOGRAPHY",
			},
			{
				Name: ident.New("enum"),
				Type: ident.NewUDT(
					ident.MustSchema(ident.New("database"), ident.New("schema")),
					ident.New("MyEnum")).String(),
			},
			{
				Name:        ident.New("has_default"),
				Type:        "INT8",
				DefaultExpr: "expr()",
			},
		},
		product: product,
		tableID: ident.NewTable(
			ident.MustSchema(ident.New("database"), ident.New("schema")),
			ident.New("table")),
	}

	switch product {
	case types.ProductCockroachDB:
		global.dir = "crdb"
	case types.ProductPostgreSQL:
		global.dir = "pg"
	default:
		t.Fatalf("unimplemented: %s", product)
	}

	tcs := []*templateTestCase{
		{
			name: "base",
		},
		{
			name: "cas",
			cfg: &applycfg.Config{
				CASColumns: []ident.Ident{ident.New("val1"), ident.New("val0")},
			},
		},
		{
			name: "deadline",
			cfg: &applycfg.Config{
				Deadlines: ident.MapOf[time.Duration](
					ident.New("val1"), time.Second,
					ident.New("val0"), time.Hour,
				),
			},
		},
		{
			name: "casDeadline",
			cfg: &applycfg.Config{
				CASColumns: []ident.Ident{ident.New("val1"), ident.New("val0")},
				Deadlines: ident.MapOf[time.Duration](
					ident.New("val0"), time.Hour,
					ident.New("val1"), time.Second,
				),
			},
		},
		{
			name: "ignore",
			cfg: &applycfg.Config{
				Ignore: ident.MapOf[bool](
					ident.New("geom"), true,
					ident.New("geog"), true,
				),
			},
		},
		{
			// Changing the source names should have no effect on the
			// SQL that gets generated; we only care about the different
			// value when looking up values in the incoming mutation.
			name: "source_names",
			cfg: &applycfg.Config{
				SourceNames: ident.MapOf[applycfg.SourceColumn](
					ident.New("val1"), ident.New("valRenamed"),
					ident.New("geog"), ident.New("george"),
					ident.New("unknown"), ident.New("is ok"),
				),
			},
		},
		{
			// Verify user-configured expressions, with zero, one, and
			// multiple uses of the substitution position.
			name: "expr",
			cfg: &applycfg.Config{
				Exprs: ident.MapOf[string](
					ident.New("val0"), `'fixed'`, // Doesn't consume a parameter slot.
					ident.New("val1"), `$0||'foobar'`,
					ident.New("pk1"), `$0+$0`,
				),
				Ignore: ident.MapOf[bool](
					ident.New("geom"), true,
					ident.New("geog"), true,
				),
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			checkTemplate(t, global, tc)
		})
	}
}

type templateGlobal struct {
	cols    []types.ColData
	dir     string
	product types.Product
	tableID ident.Table
}

type templateTestCase struct {
	name string
	cfg  *applycfg.Config
}

func checkTemplate(t *testing.T, global *templateGlobal, tc *templateTestCase) {
	t.Helper()
	r := require.New(t)
	cfg := applycfg.NewConfig()
	if tc.cfg != nil {
		cfg.Patch(tc.cfg)
	}

	mapping, err := newColumnMapping(cfg, global.cols, global.product, global.tableID)
	r.NoError(err)

	tmpls, err := newTemplates(mapping)
	r.NoError(err)

	t.Run("upsert", func(t *testing.T) {
		r := require.New(t)
		s, err := tmpls.upsertExpr(2)
		r.NoError(err)
		checkFile(t,
			fmt.Sprintf("testdata/%s/%s.upsert.sql", global.dir, tc.name),
			s)
	})

	t.Run("delete", func(t *testing.T) {
		r := require.New(t)
		s, err := tmpls.deleteExpr(2)
		r.NoError(err)
		checkFile(t,
			fmt.Sprintf("testdata/%s/%s.delete.sql", global.dir, tc.name),
			s)
	})
}

func checkFile(t *testing.T, path string, contents string) {
	t.Helper()
	r := require.New(t)

	upsertFile, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	r.NoError(err)
	defer upsertFile.Close()

	if rewriteFiles {
		_, err := upsertFile.Seek(0, 0)
		r.NoError(err)
		count, err := io.WriteString(upsertFile, contents)
		r.NoError(err)
		r.NoError(upsertFile.Truncate(int64(count)))
	} else {
		upsert, err := io.ReadAll(upsertFile)
		r.NoError(err)
		r.Equal(string(upsert), contents)
	}
}
