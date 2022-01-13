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
	"strings"
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/stretchr/testify/assert"
)

func TestQueryTemplates(t *testing.T) {
	tmpls := testTemplates()
	tcs := []struct {
		name     string
		fn       func() (string, error)
		expected string // Will have newlines stripped out, for readability.
	}{
		{"delete", tmpls.delete,
			`DELETE FROM "database"."schema"."table" WHERE ("pk0","pk1") = ($1,$2)`},
		{"upsert", tmpls.upsert,
			`UPSERT INTO "database"."schema"."table"
 ("pk0","pk1","val0","val1","geom","geog")
 VALUES (
$1::STRING,$2::INT8,$3::STRING,$4::STRING,
st_geomfromgeojson($5::JSONB),st_geogfromgeojson($6::JSONB))`},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			a := assert.New(t)
			s, err := tc.fn()
			if a.NoError(err) {
				a.Equal(strings.ReplaceAll(tc.expected, "\n", ""), s)
			}
		})

	}
}

func testTemplates() *templates {
	return templatesFor(
		ident.NewTable(
			ident.New("database"),
			ident.New("schema"),
			ident.New("table")),
		[]types.ColData{
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
		},
	)
}
