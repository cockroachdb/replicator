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
	"context"
	"regexp"
	"strconv"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/pkg/errors"
)

// Resolved is the name of table that we track resolved timestamps in.
var Resolved = ident.NewTable(ident.StagingDB, ident.Public, ident.New("resolved"))

// Example: /test/public/2020-04-04/202004042351304139680000000000000.RESOLVED
// Format is: /[targetDB]/[targetSchema]/[date]/[timestamp].RESOLVED
var (
	resolvedRegex = regexp.MustCompile(
		`^/(?P<targetDB>[^/]+)/(?P<targetSchema>[^/]+)/(?P<date>\d{4}-\d{2}-\d{2})/(?P<timestamp>\d{33}).RESOLVED$`)
	resolvedDB        = resolvedRegex.SubexpIndex("targetDB")
	resolvedSchema    = resolvedRegex.SubexpIndex("targetSchema")
	resolvedTimestamp = resolvedRegex.SubexpIndex("timestamp")
)

// resolvedURL contains all the parsed info from an ndjson url.
type resolvedURL struct {
	target    ident.Schema
	timestamp hlc.Time
}

func parseResolvedURL(url string) (resolvedURL, error) {
	match := resolvedRegex.FindStringSubmatch(url)
	if len(match) != resolvedRegex.NumSubexp()+1 {
		return resolvedURL{}, errors.Errorf("can't parse url %s", url)
	}

	resolved := resolvedURL{
		target: ident.NewSchema(ident.New(match[resolvedDB]), ident.New(match[resolvedSchema])),
	}

	tsText := match[resolvedTimestamp]
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
		return hlc.Time{}, errors.Errorf("can't parse timestamp %s", timestamp)
	}
	if len(logical) != 10 {
		return hlc.Time{}, errors.Errorf("can't parse logical timestamp %s", logical)
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

// resolved acts upon a resolved timestamp message.
func (h *Handler) resolved(ctx context.Context, r resolvedURL, immediate bool) error {
	if immediate {
		return nil
	}

	return retry.Retry(ctx, func(ctx context.Context) error {
		tx, err := h.Pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		watcher, err := h.Watchers.Get(ctx, r.target.Database())
		if err != nil {
			return err
		}
		targetTables := watcher.Snapshot(r.target)

		// Prepare to merge data.
		stores := make([]types.Stager, 0, len(targetTables))
		appliers := make([]types.Applier, 0, len(targetTables))
		for table := range targetTables {
			store, err := h.Stores.Get(ctx, table)
			if err != nil {
				return err
			}
			stores = append(stores, store)

			applier, err := h.Appliers.Get(ctx, table)
			if err != nil {
				return err
			}
			appliers = append(appliers, applier)
		}

		prev, err := h.TimeKeeper.Put(ctx, tx, r.target, r.timestamp)
		if err != nil {
			return err
		}

		if hlc.Compare(r.timestamp, prev) < 0 {
			return errors.Errorf(
				"resolved timestamp went backwards: received %s had %s",
				r.timestamp, prev)
		}

		for i := range stores {
			muts, err := stores[i].Drain(ctx, tx, prev, r.timestamp)
			if err != nil {
				return err
			}

			if err := appliers[i].Apply(ctx, tx, muts); err != nil {
				return err
			}
		}

		return tx.Commit(ctx)
	})
}
