// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package cdc contains a http.Handler which can receive
// webhook events from a CockroachDB CDC changefeed.
package cdc

import (
	"bufio"
	"context"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/sinktypes"
	"github.com/cockroachdb/cdc-sink/internal/util/batches"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
)

// This file contains code repackaged from main.go

// Handler is an http.Handler for processing webhook requests
// from a CockroachDB changefeed.
type Handler struct {
	Appliers  sinktypes.Appliers       // Update tables within TargetDb.
	Immediate bool                     // If true, apply mutations immediately.
	Pool      *pgxpool.Pool            // Access to the target cluster.
	Stores    sinktypes.MutationStores // Record incoming json blobs.
	Swapper   sinktypes.TimeSwapper    // Tracks named timestamps.
	Watchers  sinktypes.Watchers       // Schema data.
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	sendErr := func(err error) {
		if err == nil {
			http.Error(w, "OK", http.StatusOK)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Printf("ERROR %s:\n%v", r.RequestURI, err)
	}

	// Is it an ndjson url?
	if ndjson, err := parseNdjsonURL(r.RequestURI); err == nil {
		sendErr(h.ndjson(ctx, ndjson, r.Body))
	} else if resolved, err := parseResolvedURL(r.RequestURI); err == nil {
		sendErr(h.resolved(ctx, resolved))
	} else {
		http.NotFound(w, r)
	}
}

// ndjson parses an incoming block of ndjson files and stores the
// associated Mutations. This assumes that the underlying
// MutationStore will store duplicate values in an idempotent manner,
// should the request fail partway through.
func (h *Handler) ndjson(ctx context.Context, u ndjsonURL, r io.Reader) error {
	muts, release := batches.Mutation()
	defer release()

	target, err := ident.Relative(ident.New(u.targetDb), u.targetTable)
	if err != nil {
		return err
	}

	// In immediate mode, we want to apply the mutations immediately.
	// The CDC feed guarantees in-order delivery for individual rows.
	var flush func() error
	if h.Immediate {
		applier, err := h.Appliers.Get(ctx, target)
		if err != nil {
			return err
		}
		flush = func() error {
			err := applier.Apply(ctx, h.Pool, muts)
			muts = muts[:0]
			return err
		}
	} else {
		store, err := h.Stores.Get(ctx, target)
		if err != nil {
			return err
		}
		flush = func() error {
			err := store.Store(ctx, h.Pool, muts)
			muts = muts[:0]
			return err
		}
	}

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		buf := scanner.Bytes()
		if len(buf) == 0 {
			continue
		}
		mut, err := parseMutation(buf)
		if err != nil {
			return err
		}
		muts = append(muts, mut)
		if len(muts) == cap(muts) {
			flush()
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return flush()
}

// resolved acts upon a resolved timestamp message.
func (h *Handler) resolved(ctx context.Context, r resolvedURL) error {
	if h.Immediate {
		return nil
	}
	targetDb := ident.New(r.targetDb)

	return retry.Retry(ctx, func(ctx context.Context) error {
		tx, err := h.Pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		watcher, err := h.Watchers.Get(ctx, targetDb)
		if err != nil {
			return err
		}
		schema := watcher.Snapshot()

		// Prepare to merge data.
		stores := make([]sinktypes.MutationStore, 0, len(schema))
		appliers := make([]sinktypes.Applier, 0, len(schema))
		for table := range schema {
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

		prev, err := h.Swapper.Swap(ctx, tx, "_resolved_"+targetDb.Raw(), r.timestamp)
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
