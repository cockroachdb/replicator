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

// This file contains code repackaged from url.go.

import (
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/pkg/errors"
)

// Example: /test.sql/2020-04-04/202004042351304139680000000000000.RESOLVED
// Format is: /[target]/[date]/[timestamp].RESOLVED
var (
	resolvedRegex = regexp.MustCompile(
		`^/(?P<target>.*)/(?P<date>\d{4}-\d{2}-\d{2})/(?P<timestamp>\d{33}).RESOLVED$`)
	resolvedTargetIdx    = resolvedRegex.SubexpIndex("target")
	resolvedTimestampIdx = resolvedRegex.SubexpIndex("timestamp")
)

// resolvedURL contains all the parsed info from an ndjson url.
type resolvedURL struct {
	targetDB  string
	timestamp hlc.Time
}

func parseResolvedURL(url string) (resolvedURL, error) {
	match := resolvedRegex.FindStringSubmatch(url)
	if len(match) != resolvedRegex.NumSubexp()+1 {
		return resolvedURL{}, fmt.Errorf("can't parse url %s", url)
	}

	resolved := resolvedURL{
		targetDB: match[resolvedTargetIdx],
	}

	tsText := match[resolvedTimestampIdx]
	if len(tsText) != 33 {
		return resolvedURL{}, errors.Errorf(
			"expected timestamp to be 33 characters long, got %d: %s",
			len(tsText), tsText,
		)
	}
	var err error
	resolved.timestamp, err = parseResolvedTimestamp(tsText[:23], tsText[23:])
	return resolved, err
}

// This is the timestamp format:  YYYYMMDDHHMMSSNNNNNNNNNLLLLLLLLLL
// Formatting const stolen from https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/changefeedccl/sink_cloudstorage.go#L48
const timestampDateTimeFormat = "20060102150405"

func parseResolvedTimestamp(timestamp string, logical string) (hlc.Time, error) {
	if len(timestamp) != 23 {
		return hlc.Time{}, fmt.Errorf("can't parse timestamp %s", timestamp)
	}
	if len(logical) != 10 {
		return hlc.Time{}, fmt.Errorf("can't parse logical timestamp %s", logical)
	}

	// Parse the date and time.
	timestampParsed, err := time.Parse(timestampDateTimeFormat, timestamp[0:14])
	if err != nil {
		return hlc.Time{}, err
	}

	// Parse out the nanos
	nanos, err := time.ParseDuration(timestamp[14:23] + "ns")
	if err != nil {
		return hlc.Time{}, err
	}
	timestampParsed.Add(nanos)

	// Parse out the logical timestamp
	logicalParsed, err := strconv.Atoi(logical)
	if err != nil {
		return hlc.Time{}, err
	}

	return hlc.New(timestampParsed.UnixNano(), logicalParsed), nil
}
