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
	"net/url"
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

func (h *Handler) parseResolvedURL(url *url.URL, req *request) error {
	match := resolvedRegex.FindStringSubmatch(url.Path)
	if len(match) != resolvedRegex.NumSubexp()+1 {
		return errors.Errorf("can't parse url %s", url)
	}

	target := ident.NewSchema(ident.New(match[resolvedDB]), ident.New(match[resolvedSchema]))

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

	req.leaf = h.resolved
	req.target = target
	req.timestamp = timestamp
	return nil
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
func (h *Handler) resolved(ctx context.Context, req *request) error {
	if req.immediate {
		return nil
	}
	target := req.target.(ident.Schema)

	return retry.Retry(ctx, func(ctx context.Context) error {
		tx, err := h.Pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		watcher, err := h.Watchers.Get(ctx, target.Database())
		if err != nil {
			return err
		}
		// Get the tables to process, sorted based on the FK constraints
		targetTables := watcher.Snapshot().TablesSortedByFK
		stores := make([]types.Stager, 0, len(targetTables))
		appliers := make([]types.Applier, 0, len(targetTables))
		deletes := make([][]types.Mutation, 0, len(targetTables))
		// Prepare to merge data.
		for _, table := range targetTables {
			if table.AsSchema() == target {
				store, err := h.Stores.Get(ctx, table)
				if err != nil {
					return err
				}
				stores = append(stores, store)
				applier, err := h.Appliers.Get(ctx, table, req.casColumns, req.deadlines)
				if err != nil {
					return err
				}
				appliers = append(appliers, applier)
			}
		}

		prev, err := h.Swapper.Put(ctx, tx, target, req.timestamp)
		if err != nil {
			return err
		}

		if hlc.Compare(req.timestamp, prev) < 0 {
			return errors.Errorf(
				"resolved timestamp went backwards: received %s had %s",
				req.timestamp, prev)
		}
		// To support FK constraints in the target, process upserts first,
		// and accumulate deletes.
		for i := range stores {
			muts, err := stores[i].Drain(ctx, tx, prev, req.timestamp)
			if err != nil {
				return err
			}
			dmuts := make([]types.Mutation, 0, len(muts))
			umuts := make([]types.Mutation, 0, len(muts))
			for _, m := range muts {
				if m.IsDelete() {
					dmuts = append(dmuts, m)
				} else {
					umuts = append(umuts, m)
				}
			}
			deletes = append(deletes, dmuts)
			if err := appliers[i].Apply(ctx, tx, umuts); err != nil {
				return err
			}
		}
		// Delete must be processed in the opposite order
		for i := len(deletes) - 1; i >= 0; i-- {
			dmuts := deletes[i]
			if err := appliers[i].Apply(ctx, tx, dmuts); err != nil {
				return err
			}
		}
		return tx.Commit(ctx)
	})
}
