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
	"testing"
	"time"

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
	}

	tableID := ident.NewTable(
		ident.New("database"),
		ident.New("schema"),
		ident.New("table"))

	tcs := []struct {
		name   string
		cfg    *Config
		upsert string
	}{
		{
			name: "base",
			upsert: `UPSERT INTO "database"."schema"."table" (
"pk0","pk1","val0","val1","geom","geog"
) VALUES
($1::STRING,$2::INT8,$3::STRING,$4::STRING,st_geomfromgeojson($5::JSONB),st_geogfromgeojson($6::JSONB)),
($7::STRING,$8::INT8,$9::STRING,$10::STRING,st_geomfromgeojson($11::JSONB),st_geogfromgeojson($12::JSONB))`,
		},
		{
			name: "cas",
			cfg: &Config{
				CASColumns: []ident.Ident{ident.New("val1"), ident.New("val0")},
			},
			upsert: `WITH data("pk0","pk1","val0","val1","geom","geog") AS (
VALUES
($1::STRING,$2::INT8,$3::STRING,$4::STRING,st_geomfromgeojson($5::JSONB),st_geogfromgeojson($6::JSONB)),
($7::STRING,$8::INT8,$9::STRING,$10::STRING,st_geomfromgeojson($11::JSONB),st_geogfromgeojson($12::JSONB))),
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
UPSERT INTO "database"."schema"."table" ("pk0","pk1","val0","val1","geom","geog")
SELECT * FROM action`,
		},
		{
			name: "deadline",
			cfg: &Config{
				Deadlines: types.Deadlines{
					ident.New("val1"): time.Second,
					ident.New("val0"): time.Hour,
				},
			},
			upsert: `WITH data("pk0","pk1","val0","val1","geom","geog") AS (
VALUES
($1::STRING,$2::INT8,$3::STRING,$4::STRING,st_geomfromgeojson($5::JSONB),st_geogfromgeojson($6::JSONB)),
($7::STRING,$8::INT8,$9::STRING,$10::STRING,st_geomfromgeojson($11::JSONB),st_geogfromgeojson($12::JSONB))),
deadlined AS (SELECT * FROM data WHERE("val0">now()-'1h0m0s'::INTERVAL)AND("val1">now()-'1s'::INTERVAL))
UPSERT INTO "database"."schema"."table" ("pk0","pk1","val0","val1","geom","geog")
SELECT * FROM deadlined`,
		},
		{
			name: "casDeadline",
			cfg: &Config{
				CASColumns: []ident.Ident{ident.New("val1"), ident.New("val0")},
				Deadlines: types.Deadlines{
					ident.New("val0"): time.Hour,
					ident.New("val1"): time.Second,
				},
			},
			upsert: `WITH data("pk0","pk1","val0","val1","geom","geog") AS (
VALUES
($1::STRING,$2::INT8,$3::STRING,$4::STRING,st_geomfromgeojson($5::JSONB),st_geogfromgeojson($6::JSONB)),
($7::STRING,$8::INT8,$9::STRING,$10::STRING,st_geomfromgeojson($11::JSONB),st_geogfromgeojson($12::JSONB))),
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
UPSERT INTO "database"."schema"."table" ("pk0","pk1","val0","val1","geom","geog")
SELECT * FROM action`,
		},
		{
			name: "ignore",
			cfg: &Config{
				Ignore: map[SourceColumn]bool{
					ident.New("geom"): true,
					ident.New("geog"): true,
				},
			},
			upsert: `UPSERT INTO "database"."schema"."table" (
"pk0","pk1","val0","val1"
) VALUES
($1::STRING,$2::INT8,$3::STRING,$4::STRING),
($5::STRING,$6::INT8,$7::STRING,$8::STRING)`,
		},
		{
			// Changing the source names should have no effect on the
			// SQL that gets generated; we only care about the different
			// value when looking up values in the incoming mutation.
			name: "source names",
			cfg: &Config{
				SourceNames: map[TargetColumn]SourceColumn{
					ident.New("val1"):    ident.New("valRenamed"),
					ident.New("geog"):    ident.New("george"),
					ident.New("unknown"): ident.New("is ok"),
				},
			},
			upsert: `UPSERT INTO "database"."schema"."table" (
"pk0","pk1","val0","val1","geom","geog"
) VALUES
($1::STRING,$2::INT8,$3::STRING,$4::STRING,st_geomfromgeojson($5::JSONB),st_geogfromgeojson($6::JSONB)),
($7::STRING,$8::INT8,$9::STRING,$10::STRING,st_geomfromgeojson($11::JSONB),st_geogfromgeojson($12::JSONB))`,
		},
		{
			name: "expr",
			cfg: &Config{
				Exprs: map[TargetColumn]string{
					ident.New("val0"): `'fixed'`,
					ident.New("val1"): `$0||'foobar'`,
					ident.New("pk1"):  `$0+$0`,
				},
				Ignore: map[TargetColumn]bool{
					ident.New("geom"): true,
					ident.New("geog"): true,
				},
			},
			upsert: `UPSERT INTO "database"."schema"."table" (
"pk0","pk1","val0","val1"
) VALUES
($1::STRING,($2+$2)::INT8,'fixed'::STRING,($3||'foobar')::STRING),
($4::STRING,($5+$5)::INT8,'fixed'::STRING,($6||'foobar')::STRING)`,
		},
	}

	// The deletion query should never change based on configuration.
	const expectedDelete = `DELETE FROM "database"."schema"."table" WHERE ("pk0","pk1")IN(($1,$2),($3,$4))`

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			a := assert.New(t)
			if tc.cfg == nil {
				tc.cfg = NewConfig()
			}
			tmpls := newTemplates(tableID, tc.cfg, cols)

			s, err := tmpls.delete(2)
			a.NoError(err)
			a.Equal(expectedDelete, s)

			s, err = tmpls.upsert(2)
			a.NoError(err)
			a.Equal(tc.upsert, s)

		})

	}
}
