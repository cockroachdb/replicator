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
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strings"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/ndjson"
	"github.com/pkg/errors"
)

// Sets an upper bound of the number of path segments we expect.
const maxPathSegments = 8

// See https://www.cockroachlabs.com/docs/stable/create-changefeed.html#general-file-format
// Example: /2020-04-02/202004022058072107140000000000000-56087568dba1e6b8-1-72-00000000-test_table-1.ndjson
// Filename format is: [endpoint]/[date]/[timestamp + uniquer]-[topic]-[schema-id]
//
// When we look at the filename, there are five hyphen-delimited groups
// in the timestamp+uniquer segment of the filename. The topic (name of
// the table) may itself contain dashes, so it has an open match to
// consume anything that isn't the final, CRDB-internal schema id.
var (
	ndjsonRegex = regexp.MustCompile(`^(?P<date>\d{4}-\d{2}-\d{2})/(?P<prelude>([^-]+-){5})(?P<topic>.+)-(?P<schema_id>[^-]+).ndjson$`)
	ndjsonTopic = ndjsonRegex.SubexpIndex("topic")
)

// Example: /2020-04-04/202004042351304139680000000000000.RESOLVED
// Filename format is just a timestamp.
var (
	resolvedRegex     = regexp.MustCompile(`^(?P<date>\d{4}-\d{2}-\d{2})/(?P<timestamp>\d{33}).RESOLVED$`)
	resolvedTimestamp = resolvedRegex.SubexpIndex("timestamp")
)

// This is set by test code to spy on the assignment to [request.leaf],
// since some of the assignments are closures.
var requestParsingTestCallback func(decision string)

// A request is configured by the various parseURL methods in Handler.
type request struct {
	body    io.Reader
	handler *Handler
	leaf    func(ctx context.Context, req *request) error
	// keys contains all the columns that make up the primary key
	// for the target table and their ordinal position within the key.
	keys      *ident.Map[int]
	target    ident.Schematic
	timestamp hlc.Time
}

// newRequest extracts the required information from an [http.Request].
func (h *Handler) newRequest(req *http.Request) (*request, error) {
	ret := &request{
		body:    req.Body,
		handler: h,
	}
	return ret, ret.parseURL(req.URL)
}

type requestPattern struct {
	expectedPathSegments int
	pattern              *regexp.Regexp
	fn                   func(h *Handler, match []string, req *request) error
}

var requestPatterns = []*requestPattern{
	// Bulk, ndjson payload
	{
		expectedPathSegments: 2,
		pattern:              ndjsonRegex,
		fn: func(h *Handler, match []string, req *request) error {
			switch t := req.target.(type) {
			case ident.Schema:
				if requestParsingTestCallback != nil {
					requestParsingTestCallback("ndjson schema")
				}

				// Parse the topic as a (qualified) table name, then update the request.
				tbl, _, err := ident.ParseTableRelative(match[ndjsonTopic], t)
				if err != nil {
					return err
				}

				req.target = ident.NewTable(t, tbl.Table())
				req.leaf = func(ctx context.Context, req *request) error {
					return h.ndjson(ctx, req, ndjson.ParseMutation)
				}
			case ident.Table:
				if requestParsingTestCallback != nil {
					requestParsingTestCallback("ndjson table")
				}
				req.leaf = func(ctx context.Context, req *request) error {
					return h.ndjson(ctx, req, h.parseNdjsonQueryMutation(req))
				}
			default:
				return errors.Errorf("unimplemented %T", t)
			}
			return nil
		},
	},
	// Bulk, resolved payload
	{
		expectedPathSegments: 2,
		pattern:              resolvedRegex,
		fn: func(h *Handler, match []string, req *request) error {
			tsText := match[resolvedTimestamp]
			if len(tsText) != 33 {
				return errors.Errorf(
					"expected timestamp to be 33 characters long, got %d: %s",
					len(tsText), tsText,
				)
			}
			timestamp, err := parseResolvedTimestamp(tsText[:23], tsText[23:])
			if err != nil {
				return err
			}
			if requestParsingTestCallback != nil {
				requestParsingTestCallback("resolved")
			}
			req.leaf = h.resolved
			req.timestamp = timestamp
			return nil
		},
	},
	// Webhook matches anything else
	{
		fn: func(h *Handler, match []string, req *request) error {
			switch t := req.target.(type) {
			case ident.Schema:
				if requestParsingTestCallback != nil {
					requestParsingTestCallback("webhook schema")
				}
				req.leaf = h.webhook
			case ident.Table:
				if requestParsingTestCallback != nil {
					requestParsingTestCallback("webhook table")
				}
				req.leaf = h.webhookForQuery
			default:
				return errors.Errorf("unimplemented %T", t)
			}
			return nil
		},
	},
}

func (r *request) parseURL(urlInput *url.URL) error {
	// Extract path elements into a slice. We allocate the slice and
	// fill it in from the tail, due to how path.Split() operates.
	remaining := urlInput.EscapedPath()
	pathSegments := make([]string, maxPathSegments)
	segmentIdx := len(pathSegments) - 1
	for segmentIdx >= 0 && remaining != "" {
		dir, file := path.Split(remaining)
		if dir == "" {
			break
		}
		// Strip trailing separator from dir.
		remaining = dir[:len(dir)-1]
		// Ignore empty or dangling path elements e.g.: /my_db/public/
		if file == "" {
			continue
		}
		unescapedSegment, err := url.QueryUnescape(file)
		if err != nil {
			return errors.Wrap(err, "could not unescape path")
		}
		pathSegments[segmentIdx] = unescapedSegment
		segmentIdx--
	}
	if segmentIdx < 0 {
		return errors.Errorf("request path exceeded maximum of %d segments", len(pathSegments))
	}
	pathSegments = pathSegments[segmentIdx+1:]

	// Convert the expected number of path segments into a Schema.
	schemaSegmentCount := r.schemaSegmentCount()
	if len(pathSegments) < schemaSegmentCount {
		return errors.Errorf("expecting at least %d path segments", schemaSegmentCount)
	}
	schemaIdents := make([]ident.Ident, schemaSegmentCount)
	for idx, part := range pathSegments[:schemaSegmentCount] {
		schemaIdents[idx] = ident.New(part)
	}
	targetSchema, err := ident.NewSchema(schemaIdents...)
	if err != nil {
		return err
	}
	pathSegments = pathSegments[schemaSegmentCount:]

	for _, pattern := range requestPatterns {
		// If there is an extra path segment, we treat it as a table
		// name for CDC-query feeds.
		matchOn := pathSegments
		switch len(pathSegments) {
		case pattern.expectedPathSegments:
			r.target = targetSchema
		case pattern.expectedPathSegments + 1:
			r.target = ident.NewTable(targetSchema, ident.New(pathSegments[0]))
			matchOn = matchOn[1:]
		default:
			continue
		}

		// Regex test, if defined.
		var match []string
		if pattern.pattern != nil {
			patternSpecific := strings.Join(matchOn, "/")
			match = pattern.pattern.FindStringSubmatch(patternSpecific)
			if match == nil {
				continue
			}
		}

		if err := pattern.fn(r.handler, match, r); err != nil {
			continue
		}

		return nil
	}
	return errors.New("path did not match any expected patterns")
}

// schemaSegmentCount returns the number of path segments that the
// product uses for its schema namespace.
func (r *request) schemaSegmentCount() int {
	switch r.handler.TargetPool.Product {
	case types.ProductUnknown:
		return 0
	case types.ProductOracle, types.ProductMariaDB, types.ProductMySQL:
		return 1 // e.g. MY_SCHEMA
	case types.ProductCockroachDB, types.ProductPostgreSQL:
		return 2 // e.g. MY_DB.MY_SCHEMA
	default:
		panic(fmt.Sprintf("unimplemented: %s", r.handler.TargetPool.Product))
	}
}
