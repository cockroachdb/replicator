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

// This file contains code repackaged from url.go.

import (
	"bytes"
	"context"
	"encoding/json"
	"net/url"
	"regexp"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
)

// See https://www.cockroachlabs.com/docs/stable/create-changefeed.html#general-file-format
// Example: /targetDB/targetSchema/targetTable/2020-04-02/202004022058072107140000000000000-56087568dba1e6b8-1-72-00000000-test_table-1.ndjson
// Format is: /[endpoint]/[date]/[timestamp]-[uniquer]-[topic]-[schema-id]
var (
	ndjsonQueryRegex        = regexp.MustCompile(`^/(?P<targetDB>[^/]+)/(?P<targetSchema>[^/]+)/(?P<targetTable>[^/]+)/(?P<date>\d{4}-\d{2}-\d{2})/(?P<uniquer>.+)-(?P<topic>[^-]+)-(?P<schema_id>[^-]+).ndjson$`)
	ndjsonQueryTargetDB     = ndjsonQueryRegex.SubexpIndex("targetDB")
	ndjsonQueryTargetSchema = ndjsonQueryRegex.SubexpIndex("targetSchema")
	ndjsonQueryTargetTable  = ndjsonQueryRegex.SubexpIndex("targetTable")
)

func (h *Handler) parseNdjsonQueryURL(url *url.URL, req *request) error {
	match := ndjsonQueryRegex.FindStringSubmatch(url.Path)
	if match == nil {
		return errors.Errorf("can't parse url %s", url)
	}
	// for CDC queries, we rely on table from the URL Path, for consistency with webhook
	table := ident.NewTable(
		ident.New(match[ndjsonQueryTargetDB]),
		ident.New(match[ndjsonQueryTargetSchema]),
		ident.New(match[ndjsonQueryTargetTable]),
	)
	req.leaf = func(ctx context.Context, req *request) error {
		return h.ndjson(ctx, req, h.parseQueryMutation)
	}
	req.target = table
	return nil

}

// parseQueryMutation takes a single line from an ndjson and extracts enough
// information to be able to persist it to the staging table.
// When using CDC queries the SELECT Statement must include the event (as "__event__")
// returned by the event_op() function.
// SELECT *, event_op() as operation
// See (https://www.cockroachlabs.com/docs/stable/cdc-queries.html#cdc-query-function-support)
func (h *Handler) parseQueryMutation(
	ctx context.Context, req *request, rawBytes []byte,
) (types.Mutation, error) {
	keys, err := req.getPrimaryKey(ctx, h.Resolvers.watchers)
	if err != nil {
		return types.Mutation{}, err
	}
	qp := queryPayload{
		keys: keys,
	}
	dec := json.NewDecoder(bytes.NewReader(rawBytes))
	if err := dec.Decode(&qp); err != nil {
		return types.Mutation{}, err
	}
	return qp.AsMutation()
}
