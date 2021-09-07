// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// This is the timestamp format:  YYYYMMDDHHMMSSNNNNNNNNNLLLLLLLLLL
// Formatting const stolen from https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/changefeedccl/sink_cloudstorage.go#L48
const timestampDateTimeFormat = "20060102150405"

func parseTimestamp(timestamp string, logical string) (time.Time, int, error) {
	if len(timestamp) != 23 {
		return time.Time{}, 0, fmt.Errorf("Can't parse timestamp %s", timestamp)
	}
	if len(logical) != 10 {
		return time.Time{}, 0, fmt.Errorf("Can't parse logical timestamp %s", logical)
	}

	// Parse the date and time.
	timestampParsed, err := time.Parse(timestampDateTimeFormat, timestamp[0:14])
	if err != nil {
		return time.Time{}, 0, err
	}

	// Parse out the nanos
	nanos, err := time.ParseDuration(timestamp[14:23] + "ns")
	if err != nil {
		return time.Time{}, 0, err
	}
	timestampParsed.Add(nanos)

	// Parse out the logical timestamp
	logicalParsed, err := strconv.Atoi(logical)
	if err != nil {
		return time.Time{}, 0, err
	}

	return timestampParsed, logicalParsed, nil
}

// See https://www.cockroachlabs.com/docs/stable/create-changefeed.html#general-file-format
// Example: /test.sql//2020-04-02/202004022058072107140000000000000-56087568dba1e6b8-1-72-00000000-test_table-1.ndjson
// Format is: /[endpoint]/[date]/[timestamp]-[uniquer]-[topic]-[schema-id]
var (
	ndjsonRegex       = regexp.MustCompile(`/(?P<endpoint>[^/]*)/(?P<date>\d{4}-\d{2}-\d{2})/(?P<uniquer>.+)-(?P<topic>[^-]+)-(?P<schema_id>[^-]+).ndjson$`)
	ndjsonEndpointIdx = ndjsonRegex.SubexpIndex("endpoint")
	ndjsonTopicIdx    = ndjsonRegex.SubexpIndex("topic")
)

// ndjsonURL contains all the parsed info from an ndjson url.
type ndjsonURL struct {
	endpoint string
	topic    string
}

func parseNdjsonURL(url string) (ndjsonURL, error) {
	match := ndjsonRegex.FindStringSubmatch(url)
	if match == nil {
		return ndjsonURL{}, fmt.Errorf("can't parse url %s", url)
	}

	return ndjsonURL{
		endpoint: match[ndjsonEndpointIdx],
		topic:    match[ndjsonTopicIdx],
	}, nil
}

// Example: /test.sql/2020-04-04/202004042351304139680000000000000.RESOLVED
// Format is: /[endpoint]/[date]/[timestamp].RESOLVED
var resolvedRegex = regexp.MustCompile(`^/(?P<endpoint>.*)/(?P<date>\d{4}-\d{2}-\d{2})/(?P<timestamp>\d{33}).RESOLVED$`)

// resolvedURL contains all the parsed info from an ndjson url.
type resolvedURL struct {
	endpoint         string
	date             string
	timestamp        time.Time
	timestampLogical int
}

func parseResolvedURL(url string) (resolvedURL, error) {
	match := resolvedRegex.FindStringSubmatch(url)
	if len(match) != resolvedRegex.NumSubexp()+1 {
		return resolvedURL{}, fmt.Errorf("can't parse url %s", url)
	}

	var resolved resolvedURL
	for i, name := range resolvedRegex.SubexpNames() {
		switch name {
		case "date":
			resolved.date = strings.ToLower(match[i])
		case "timestamp":
			if len(match[i]) != 33 {
				return resolvedURL{}, fmt.Errorf(
					"Expected timestamp to be 33 characters long, got %d: %s",
					len(match[i]), match[i],
				)
			}
			var err error
			resolved.timestamp, resolved.timestampLogical, err = parseTimestamp(
				match[i][0:23], match[i][23:33],
			)
			if err != nil {
				return resolvedURL{}, err
			}
		case "endpoint":
			resolved.endpoint = strings.ToLower(match[i])
		default:
			// Skip all the rest.
		}
	}

	return resolved, nil
}
