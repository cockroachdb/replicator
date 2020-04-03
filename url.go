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

// Example: /test.sql/2020-04-02/202004022058072107140000000000000-56087568dba1e6b8-1-72-00000000-_test_table_4064-1.ndjson
// Format is: /[endpoint]/[date]/[timestamp]-[uniquer]-[topic]-[schame-id]
// var ndjsonRegex = regexp.MustCompile(`/(?P<date>\d{4}-\d{2}-\d{2})/(?P<timestamp>\d{33})-(?P<uniquer>[0-9a-g]*-\d*-\d*-\d*)-(?P<topic>.*)-(?P<schema_id>\d*).ndjson$`)
var ndjsonRegex = regexp.MustCompile(`/(?P<date>\d{4}-\d{2}-\d{2})/(?P<timestamp>\d{33})-(?P<uniquer>(?P<session_id>[0-9a-g]*)-(?P<node_id>\d*)-(?P<sink_id>\d*)-(?P<file_id>\d*))-(?P<topic>.*)-(?P<schema_id>\d*).ndjson$`)

// YYYYMMDDHHMMSSNNNNNNNNNLLLLLLLLLL
// formatting const stolen from https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/changefeedccl/sink_cloudstorage.go#L48
const timestampDateTimeFormat = "20060102150405"

// Ndjson contains all the parsed info from an ndjson url.
type Ndjson struct {
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

func parseNdjsonURL(url string) (Ndjson, error) {
	match := ndjsonRegex.FindStringSubmatch(url)
	if len(match) != ndjsonRegex.NumSubexp()+1 {
		return Ndjson{}, fmt.Errorf("can't parse url %s", url)
	}

	var ndjson Ndjson
	for i, name := range ndjsonRegex.SubexpNames() {
		switch name {
		case "date":
			ndjson.date = strings.ToLower(match[i])
		case "timestamp":
			if len(match[i]) != 33 {
				return Ndjson{}, fmt.Errorf(
					"Expected timestamp to be 33 characters long, got %d: %s",
					len(match[i]), match[i],
				)
			}

			// Parse the date and time.
			var err error
			ndjson.timestamp, err = time.Parse(timestampDateTimeFormat, match[i][0:14])
			if err != nil {
				return Ndjson{}, err
			}

			log.Printf("time: %s", match[i][0:14])
			log.Printf("time: %s", ndjson.timestamp.Format(timestampDateTimeFormat))

			// Parse out the nanos
			nanos, err := time.ParseDuration(match[i][14:23] + "ns")
			if err != nil {
				return Ndjson{}, err
			}
			ndjson.timestamp.Add(nanos)

			log.Printf("nanos: %s", match[i][14:23])
			log.Printf("nanos: %d", nanos.Nanoseconds())

			// Parse out the logical timestamp
			ndjson.timestampLogical, err = strconv.Atoi(match[i][23:])
			if err != nil {
				return Ndjson{}, err
			}
			log.Printf("logical: %s", match[i][23:33])
			log.Printf("logical: %d", ndjson.timestampLogical)

		case "uniquer":
			ndjson.uniquer = strings.ToLower(match[i])
		case "session_id":
			ndjson.sessionID = strings.ToLower(match[i])
		case "node_id":
			result, err := strconv.Atoi(match[i])
			if err != nil {
				return Ndjson{}, err
			}
			ndjson.nodeID = result
		case "sink_id":
			result, err := strconv.Atoi(match[i])
			if err != nil {
				return Ndjson{}, err
			}
			ndjson.sinkID = result
		case "file_id":
			result, err := strconv.Atoi(match[i])
			if err != nil {
				return Ndjson{}, err
			}
			ndjson.fileID = result
		case "topic":
			ndjson.topic = strings.ToLower(match[i])
		case "schema_id":
			result, err := strconv.Atoi(match[i])
			if err != nil {
				return Ndjson{}, err
			}
			ndjson.schemaID = result
		default:
			// Skip all the rest.
		}
	}

	log.Printf("url: %+v", url)
	log.Printf("ndjson: %+v", ndjson)

	return ndjson, nil
}
