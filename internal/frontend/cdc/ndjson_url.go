// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cdc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/cockroachdb/cdc-sink/internal/sinktypes"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/pkg/errors"
)

// See https://www.cockroachlabs.com/docs/stable/create-changefeed.html#general-file-format
// Example: /target//2020-04-02/202004022058072107140000000000000-56087568dba1e6b8-1-72-00000000-test_table-1.ndjson
// Format is: /[endpoint]/[date]/[timestamp]-[uniquer]-[topic]-[schema-id]
var (
	ndjsonRegex       = regexp.MustCompile(`/(?P<target>[^/]*)/(?P<date>\d{4}-\d{2}-\d{2})/(?P<uniquer>.+)-(?P<topic>[^-]+)-(?P<schema_id>[^-]+).ndjson$`)
	ndjsonEndpointIdx = ndjsonRegex.SubexpIndex("target")
	ndjsonTopicIdx    = ndjsonRegex.SubexpIndex("topic")
)

// ndjsonURL contains all the parsed info from an ndjson url.
type ndjsonURL struct {
	targetDB    string
	targetTable string
}

func parseNdjsonURL(url string) (ndjsonURL, error) {
	match := ndjsonRegex.FindStringSubmatch(url)
	if match == nil {
		return ndjsonURL{}, fmt.Errorf("can't parse url %s", url)
	}

	return ndjsonURL{
		targetDB:    match[ndjsonEndpointIdx],
		targetTable: match[ndjsonTopicIdx],
	}, nil
}

// parseMutation takes a single line from an ndjson and extracts enough
// information to be able to persist it to the staging table.
func parseMutation(rawBytes []byte) (sinktypes.Mutation, error) {
	var payload struct {
		After   json.RawMessage `json:"after"`
		Key     json.RawMessage `json:"key"`
		Updated string          `json:"updated"`
	}

	// Large numbers are not turned into strings, so the UseNumber option for
	// the decoder is required.
	dec := json.NewDecoder(bytes.NewReader(rawBytes))
	dec.UseNumber()
	if err := dec.Decode(&payload); err != nil {
		return sinktypes.Mutation{}, err
	}

	if payload.Updated == "" {
		return sinktypes.Mutation{},
			errors.New("CREATE CHANGEFEED must specify the 'WITH updated' option")
	}

	// Parse the timestamp into nanos and logical.
	ts, err := hlc.Parse(payload.Updated)
	if err != nil {
		return sinktypes.Mutation{}, err
	}

	return sinktypes.Mutation{
		Time: ts,
		Data: payload.After,
		Key:  payload.Key,
	}, nil
}
