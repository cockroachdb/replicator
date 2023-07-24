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

package cdc

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseChangefeedURL(t *testing.T) {
	schemaIdent := ident.MustSchema(ident.New("database"), ident.New("schema"))
	tableIdent := ident.NewTable(schemaIdent, ident.New("table"))
	nameParts := tableIdent.Idents(nil)
	require.Len(t, nameParts, 3)
	dbName := nameParts[0].Raw()
	schemaName := nameParts[1].Raw()
	tableName := nameParts[2].Raw()
	ndjson := strings.ReplaceAll(
		`2020-04-02/202004022058072107140000000000000-56087568dba1e6b8-1-72-00000000-REAL-1.ndjson`,
		"TABLE", tableName)
	ndjsonFull := strings.ReplaceAll(
		`2020-04-02/202004022058072107140000000000000-56087568dba1e6b8-1-72-00000000-ignored_db.ignored_schema.REAL-1.ndjson`,
		"TABLE", tableName)
	resolved := `2020-04-04/202004042351304139680000000000000.RESOLVED`

	tests := []struct {
		name      string
		decision  string
		timestamp hlc.Time
		target    ident.Schematic
		url       string
		wantErr   string
	}{
		{
			name:    "empty",
			target:  schemaIdent,
			wantErr: "expecting at least 2 path segments",
		},
		{
			name:    "root",
			target:  schemaIdent,
			url:     "/",
			wantErr: "expecting at least 2 path segments",
		},
		{
			name:    "too short",
			target:  schemaIdent,
			url:     strings.Join([]string{"", schemaName}, "/"),
			wantErr: "expecting at least 2 path segments",
		},
		{
			name:    "too long",
			target:  schemaIdent,
			url:     strings.Join([]string{"", "1", "2", "3", "4", "5", "6", "7", "8"}, "/"),
			wantErr: "request path exceeded maximum of 8 segments",
		},
		{
			name:     "webhook to schema",
			decision: "webhook schema",
			target:   schemaIdent,
			url:      strings.Join([]string{"", dbName, schemaName}, "/"),
		},
		{
			name:     "webhook to table",
			decision: "webhook table",
			target:   tableIdent,
			url:      strings.Join([]string{"", dbName, schemaName, tableName}, "/"),
		},
		{
			name:      "resolved to schema",
			decision:  "resolved",
			target:    schemaIdent,
			timestamp: hlc.New(1586044290413968000, 0),
			url:       strings.Join([]string{"", dbName, schemaName, resolved}, "/"),
		},
		{
			name:      "resolved to table",
			decision:  "resolved",
			target:    tableIdent,
			timestamp: hlc.New(1586044290413968000, 0),
			url:       strings.Join([]string{"", dbName, schemaName, tableName, resolved}, "/"),
		},
		{
			name:    "resolved too short",
			url:     strings.Join([]string{"", dbName, resolved}, "/"),
			wantErr: "expecting the first 2 path segments to be schema names",
		},
		{
			name:    "resolved too long",
			url:     strings.Join([]string{"", dbName, schemaName, "too", "much", resolved}, "/"),
			wantErr: "path did not match any expected patterns",
		},
		{
			name:     "ndjson to schema",
			decision: "ndjson schema",
			target:   ident.NewTable(schemaIdent, ident.New("REAL")), // Use topic name from query.
			url:      strings.Join([]string{"", dbName, schemaName, ndjson}, "/"),
		},
		{
			name:     "ndjson full to schema",
			decision: "ndjson schema",
			target:   ident.NewTable(schemaIdent, ident.New("REAL")), // Use topic name from query.
			url:      strings.Join([]string{"", dbName, schemaName, ndjsonFull}, "/"),
		},
		{
			name:    "ndjson too short",
			url:     strings.Join([]string{"", dbName, ndjson}, "/"),
			wantErr: "expecting the first 2 path segments to be schema names",
		},
		{
			name:    "ndjson too long",
			url:     strings.Join([]string{"", dbName, schemaName, "too", "much", ndjson}, "/"),
			wantErr: "path did not match any expected patterns",
		},
		{
			name:     "ndjson to table",
			decision: "ndjson table",
			target:   tableIdent,
			url:      strings.Join([]string{"", dbName, schemaName, tableName, ndjson}, "/"),
		},
		{
			name:     "ndjson full to table",
			decision: "ndjson table",
			target:   tableIdent,
			url:      strings.Join([]string{"", dbName, schemaName, tableName, ndjsonFull}, "/"),
		},
	}

	h := &Handler{
		TargetPool: &types.TargetPool{
			PoolInfo: types.PoolInfo{Product: types.ProductCockroachDB},
		},
	}
	var leafDecision string
	requestParsingTestCallback = func(decision string) { leafDecision = decision }

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := assert.New(t)
			r := require.New(t)

			theURL, err := url.Parse(tt.url)
			r.NoError(err)

			request, err := h.newRequest(&http.Request{URL: theURL})
			if tt.wantErr != "" {
				a.ErrorContains(err, tt.wantErr)
				return
			}
			a.NoError(err)
			a.Equal(tt.decision, leafDecision)
			a.Equalf(tt.target, request.target, "%s vs %s", tt.target, request.target)
			a.Equal(tt.timestamp, request.timestamp)
		})
	}
}
