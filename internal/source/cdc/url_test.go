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

// This file contains code repackaged from url_test.go.

import (
	"net/url"
	"strings"
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestURL(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)
	fixture, tableInfo := createFixture(t, false)
	tableName := tableInfo.Name().Table().Raw()
	dbName := tableInfo.Name().Database().Raw()
	schemaName := tableInfo.Name().Schema().Raw()
	schema := ident.NewSchema(
		ident.New(dbName),
		ident.New(schemaName),
	)
	table := ident.NewTable(
		ident.New(dbName),
		ident.New(schemaName),
		ident.New(tableName),
	)
	parseError := "can't parse url"
	ndjson := strings.ReplaceAll(
		`2020-04-02/202004022058072107140000000000000-56087568dba1e6b8-1-72-00000000-TABLE-1.ndjson`,
		"TABLE", tableName)
	ndjsonFull := strings.ReplaceAll(
		`2020-04-02/202004022058072107140000000000000-56087568dba1e6b8-1-72-00000000-ignored_db.ignored_schema.TABLE-1.ndjson`,
		"TABLE", tableName)
	resolved := `2020-04-04/202004042351304139680000000000000.RESOLVED`

	handler := fixture.Handler
	tests := []struct {
		name      string
		URL       string
		f         func(*url.URL, *request) error
		want      ident.Schematic
		timestamp hlc.Time
		wantErr   string
	}{
		{
			"webhook",
			strings.Join([]string{"", dbName, schemaName}, "/"),
			handler.parseWebhookURL,
			schema,
			hlc.Zero(),
			"",
		},
		{
			"webhook fail",
			strings.Join([]string{"", schemaName}, "/"),
			handler.parseWebhookURL,
			schema,
			hlc.Zero(),
			parseError,
		},
		{
			"resolved",
			strings.Join([]string{"", dbName, schemaName, resolved}, "/"),
			handler.parseResolvedURL,
			schema,
			hlc.New(1586044290413968000, 0),
			"",
		},
		{
			"resolved fail",
			strings.Join([]string{"", schemaName, resolved}, "/"),
			handler.parseResolvedURL,
			nil,
			hlc.Zero(),
			parseError,
		},
		{
			"ndjson",
			strings.Join([]string{"", dbName, schemaName, ndjson}, "/"),
			handler.parseNdjsonURL,
			table,
			hlc.Zero(),
			"",
		},
		{
			"ndjson-full",
			strings.Join([]string{"", dbName, schemaName, ndjsonFull}, "/"),
			handler.parseNdjsonURL,
			table,
			hlc.Zero(),
			"",
		},
		{
			"ndjson no db",
			strings.Join([]string{"", schemaName, ndjson}, "/"),
			handler.parseNdjsonURL,
			nil,
			hlc.Zero(),
			parseError,
		},
		{
			"webhook-query",
			strings.Join([]string{"", dbName, schemaName, tableName}, "/"),
			handler.parseWebhookQueryURL,
			table,
			hlc.Zero(),
			"",
		},
		{
			"webhook-query fail",
			strings.Join([]string{"", dbName, schemaName}, "/"),
			handler.parseWebhookQueryURL,
			nil,
			hlc.Zero(),
			parseError,
		},
		{
			"resolved-query",
			strings.Join([]string{"", dbName, schemaName, tableName, resolved}, "/"),
			handler.parseResolvedQueryURL,
			table,
			hlc.New(1586044290413968000, 0),
			"",
		},
		{
			"resolved-query-error",
			strings.Join([]string{"", dbName, schemaName, resolved}, "/"),
			handler.parseResolvedQueryURL,
			nil,
			hlc.Zero(),
			parseError,
		},
		{
			"ndjson-query",
			strings.Join([]string{"", dbName, schemaName, tableName, ndjson}, "/"),
			handler.parseNdjsonQueryURL,
			table,
			hlc.Zero(),
			"",
		},
		{
			"ndjson-full-query",
			strings.Join([]string{"", dbName, schemaName, tableName, ndjsonFull}, "/"),
			handler.parseNdjsonQueryURL,
			table,
			hlc.Zero(),
			"",
		},
		{
			"ndjson-query error",
			strings.Join([]string{"", dbName, schemaName, ndjson}, "/"),
			handler.parseNdjsonQueryURL,
			nil,
			hlc.Zero(),
			parseError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := &request{}
			theURL, err := url.Parse(tt.URL)
			r.NoError(err)
			err = tt.f(theURL, request)
			if tt.wantErr != "" {
				a.ErrorContains(err, tt.wantErr)
				return
			}
			a.Equal(tt.want, request.target)
			a.Equal(tt.timestamp, request.timestamp)
		})
	}
}
