package main

import (
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
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

// Example: /test.sql/2020-04-02/202004022058072107140000000000000-56087568dba1e6b8-1-72-00000000-_test_table_4064-1.ndjson
// Format is: /[endpoint]/[date]/[timestamp]-[uniquer]-[topic]-[schema-id]
// See https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/changefeedccl/sink_cloudstorage.go#L139
var ndjsonRegex = regexp.MustCompile(`^/(?P<endpoint>.*)/(?P<date>\d{4}-\d{2}-\d{2})/(?P<timestamp>\d{33})-(?P<uniquer>(?P<session_id>[0-9a-g]*)-(?P<node_id>\d*)-(?P<sink_id>\d*)-(?P<file_id>\d*))-(?P<topic>.*)-(?P<schema_id>\d*).ndjson$`)

// ndjsonURL contains all the parsed info from an ndjson url.
type ndjsonURL struct {
	endpoint         string
	date             string
	timestamp        time.Time
	timestampLogical int
	uniquer          string
	fileID           int
	nodeID           int
	sinkID           int
	sessionID        string
	topic            string
	schemaID         int
}

func parseNdjsonURL(url string) (ndjsonURL, error) {
	match := ndjsonRegex.FindStringSubmatch(url)
	if len(match) != ndjsonRegex.NumSubexp()+1 {
		return ndjsonURL{}, fmt.Errorf("can't parse url %s", url)
	}

	var ndjson ndjsonURL
	for i, name := range ndjsonRegex.SubexpNames() {
		switch name {
		case "date":
			ndjson.date = strings.ToLower(match[i])
		case "timestamp":
			if len(match[i]) != 33 {
				return ndjsonURL{}, fmt.Errorf(
					"Expected timestamp to be 33 characters long, got %d: %s",
					len(match[i]), match[i],
				)
			}
			var err error
			ndjson.timestamp, ndjson.timestampLogical, err = parseTimestamp(
				match[i][0:23], match[i][23:33],
			)
			if err != nil {
				return ndjsonURL{}, err
			}
		case "uniquer":
			ndjson.uniquer = strings.ToLower(match[i])
		case "session_id":
			ndjson.sessionID = strings.ToLower(match[i])
		case "node_id":
			result, err := strconv.Atoi(match[i])
			if err != nil {
				return ndjsonURL{}, err
			}
			ndjson.nodeID = result
		case "sink_id":
			result, err := strconv.Atoi(match[i])
			if err != nil {
				return ndjsonURL{}, err
			}
			ndjson.sinkID = result
		case "file_id":
			result, err := strconv.Atoi(match[i])
			if err != nil {
				return ndjsonURL{}, err
			}
			ndjson.fileID = result
		case "topic":
			ndjson.topic = strings.ToLower(match[i])
		case "schema_id":
			result, err := strconv.Atoi(match[i])
			if err != nil {
				return ndjsonURL{}, err
			}
			ndjson.schemaID = result
		case "endpoint":
			ndjson.endpoint = strings.ToLower(match[i])
		default:
			// Skip all the rest.
		}
	}

	log.Printf("url: %+v", url)
	log.Printf("ndjson: %+v", ndjson)

	return ndjson, nil
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

	log.Printf("url: %+v", url)
	log.Printf("resolved: %+v", resolved)

	return resolved, nil
}
