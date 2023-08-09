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
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/staging/applycfg"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/stretchr/testify/assert"
)

func TestQueryTemplates(t *testing.T) {
	cols := []types.ColData{
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
				ident.New("MyEnum")),
		},
	}

	tableID := ident.NewTable(
		ident.MustSchema(ident.New("database"), ident.New("schema")),
		ident.New("table"))

	const typicalDelete = `DELETE FROM "database"."schema"."table" WHERE ("pk0","pk1","ignored_pk")IN(($1::STRING,$2::INT8,$3::STRING),
($4::STRING,$5::INT8,$6::STRING))`

	tcs := []struct {
		name   string
		cfg    *applycfg.Config
		delete string
		upsert string
	}{
		{
			name: "base",
			upsert: `UPSERT INTO "database"."schema"."table" (
"pk0","pk1","val0","val1","geom","geog","enum"
) VALUES
($1::STRING,$2::INT8,$3::STRING,$4::STRING,st_geomfromgeojson($5::JSONB),st_geogfromgeojson($6::JSONB),$7::"database"."schema"."MyEnum"),
($8::STRING,$9::INT8,$10::STRING,$11::STRING,st_geomfromgeojson($12::JSONB),st_geogfromgeojson($13::JSONB),$14::"database"."schema"."MyEnum")`,
		},
		{
			name: "cas",
			cfg: &applycfg.Config{
				CASColumns: []ident.Ident{ident.New("val1"), ident.New("val0")},
			},
			upsert: `WITH data("pk0","pk1","val0","val1","geom","geog","enum") AS (
VALUES
($1::STRING,$2::INT8,$3::STRING,$4::STRING,st_geomfromgeojson($5::JSONB),st_geogfromgeojson($6::JSONB),$7::"database"."schema"."MyEnum"),
($8::STRING,$9::INT8,$10::STRING,$11::STRING,st_geomfromgeojson($12::JSONB),st_geogfromgeojson($13::JSONB),$14::"database"."schema"."MyEnum")),
current AS (
SELECT "pk0","pk1", "table"."val1","table"."val0"
FROM "database"."schema"."table"
JOIN data
USING ("pk0","pk1")),
action AS (
SELECT data.* FROM data
LEFT JOIN current
USING ("pk0","pk1")
WHERE current."pk0" IS NULL OR
("data"."val1","data"."val0") > ("current"."val1","current"."val0"))
UPSERT INTO "database"."schema"."table" ("pk0","pk1","val0","val1","geom","geog","enum")
SELECT * FROM action`,
		},
		{
			name: "deadline",
			cfg: &applycfg.Config{
				Deadlines: ident.MapOf[time.Duration](
					ident.New("val1"), time.Second,
					ident.New("val0"), time.Hour,
				),
			},
			upsert: `WITH data("pk0","pk1","val0","val1","geom","geog","enum") AS (
VALUES
($1::STRING,$2::INT8,$3::STRING,$4::STRING,st_geomfromgeojson($5::JSONB),st_geogfromgeojson($6::JSONB),$7::"database"."schema"."MyEnum"),
($8::STRING,$9::INT8,$10::STRING,$11::STRING,st_geomfromgeojson($12::JSONB),st_geogfromgeojson($13::JSONB),$14::"database"."schema"."MyEnum")),
deadlined AS (SELECT * FROM data WHERE("val0">now()-'1h0m0s'::INTERVAL)AND("val1">now()-'1s'::INTERVAL))
UPSERT INTO "database"."schema"."table" ("pk0","pk1","val0","val1","geom","geog","enum")
SELECT * FROM deadlined`,
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
			upsert: `WITH data("pk0","pk1","val0","val1","geom","geog","enum") AS (
VALUES
($1::STRING,$2::INT8,$3::STRING,$4::STRING,st_geomfromgeojson($5::JSONB),st_geogfromgeojson($6::JSONB),$7::"database"."schema"."MyEnum"),
($8::STRING,$9::INT8,$10::STRING,$11::STRING,st_geomfromgeojson($12::JSONB),st_geogfromgeojson($13::JSONB),$14::"database"."schema"."MyEnum")),
deadlined AS (SELECT * FROM data WHERE("val0">now()-'1h0m0s'::INTERVAL)AND("val1">now()-'1s'::INTERVAL)),
current AS (
SELECT "pk0","pk1", "table"."val1","table"."val0"
FROM "database"."schema"."table"
JOIN deadlined
USING ("pk0","pk1")),
action AS (
SELECT deadlined.* FROM deadlined
LEFT JOIN current
USING ("pk0","pk1")
WHERE current."pk0" IS NULL OR
("deadlined"."val1","deadlined"."val0") > ("current"."val1","current"."val0"))
UPSERT INTO "database"."schema"."table" ("pk0","pk1","val0","val1","geom","geog","enum")
SELECT * FROM action`,
		},
		{
			name: "ignore",
			cfg: &applycfg.Config{
				Ignore: ident.MapOf[bool](
					ident.New("geom"), true,
					ident.New("geog"), true,
				),
			},
			upsert: `UPSERT INTO "database"."schema"."table" (
"pk0","pk1","val0","val1","enum"
) VALUES
($1::STRING,$2::INT8,$3::STRING,$4::STRING,$5::"database"."schema"."MyEnum"),
($6::STRING,$7::INT8,$8::STRING,$9::STRING,$10::"database"."schema"."MyEnum")`,
		},
		{
			// Changing the source names should have no effect on the
			// SQL that gets generated; we only care about the different
			// value when looking up values in the incoming mutation.
			name: "source names",
			cfg: &applycfg.Config{
				SourceNames: ident.MapOf[applycfg.SourceColumn](
					ident.New("val1"), ident.New("valRenamed"),
					ident.New("geog"), ident.New("george"),
					ident.New("unknown"), ident.New("is ok"),
				),
			},
			upsert: `UPSERT INTO "database"."schema"."table" (
"pk0","pk1","val0","val1","geom","geog","enum"
) VALUES
($1::STRING,$2::INT8,$3::STRING,$4::STRING,st_geomfromgeojson($5::JSONB),st_geogfromgeojson($6::JSONB),$7::"database"."schema"."MyEnum"),
($8::STRING,$9::INT8,$10::STRING,$11::STRING,st_geomfromgeojson($12::JSONB),st_geogfromgeojson($13::JSONB),$14::"database"."schema"."MyEnum")`,
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
			delete: `DELETE FROM "database"."schema"."table" WHERE ("pk0","pk1","ignored_pk")IN(($1::STRING,($2+$2)::INT8,$3::STRING),
($4::STRING,($5+$5)::INT8,$6::STRING))`,
			upsert: `UPSERT INTO "database"."schema"."table" (
"pk0","pk1","val0","val1","enum"
) VALUES
($1::STRING,($2+$2)::INT8,('fixed')::STRING,($3||'foobar')::STRING,$4::"database"."schema"."MyEnum"),
($5::STRING,($6+$6)::INT8,('fixed')::STRING,($7||'foobar')::STRING,$8::"database"."schema"."MyEnum")`,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			a := assert.New(t)
			cfg := applycfg.NewConfig()
			if tc.cfg != nil {
				cfg.Patch(tc.cfg)
			}
			tmpls := newTemplates(tableID, cfg, cols)

			s, err := tmpls.delete(2)
			a.NoError(err)
			if tc.delete == "" {
				a.Equal(typicalDelete, s)
			} else {
				a.Equal(tc.delete, s)
			}

			s, err = tmpls.upsert(2)
			a.NoError(err)
			a.Equal(tc.upsert, s)
		})

	}
}
