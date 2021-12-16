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
	"log"
	"net/http"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/jackc/pgx/v4/pgxpool"
)

// This file contains code repackaged from main.go

// Handler is an http.Handler for processing webhook requests
// from a CockroachDB changefeed.
type Handler struct {
	Appliers  types.Appliers       // Update tables within TargetDb.
	Immediate bool                 // If true, apply mutations immediately.
	Pool      *pgxpool.Pool        // Access to the target cluster.
	Stores    types.MutationStores // Record incoming json blobs.
	Swapper   types.TimeKeeper     // Tracks named timestamps.
	Watchers  types.Watchers       // Schema data.
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	sendErr := func(err error) {
		if err == nil {
			http.Error(w, "OK", http.StatusOK)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.Printf("ERROR %s:\n%v", r.RequestURI, err)
	}

	if ndjson, err := parseNdjsonURL(r.RequestURI); err == nil {
		sendErr(h.ndjson(ctx, ndjson, r.Body))
		return
	}
	if resolved, err := parseResolvedURL(r.RequestURI); err == nil {
		sendErr(h.resolved(ctx, resolved))
		return
	}
	if webhook, err := parseWebhookURL(r.RequestURI); err == nil {
		if r.Method == "POST" && r.Header.Get("content-type") == "application/json" {
			sendErr(h.webhook(ctx, webhook, r.Body))
			return
		}
	}

	http.NotFound(w, r)
}
