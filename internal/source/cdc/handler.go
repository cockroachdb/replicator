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
	"net/http"
	"strconv"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/jackc/pgx/v4/pgxpool"
	log "github.com/sirupsen/logrus"
)

// This file contains code repackaged from main.go

// ImmediateParam is the name of a query parameter that will place a request
// into "immediate" mode.  In this mode, mutations are written directly
// to the target table and resolved timestamps are ignored.
const ImmediateParam = "immediate"

// Handler is an http.Handler for processing webhook requests
// from a CockroachDB changefeed.
type Handler struct {
	Appliers   types.Appliers   // Update tables within TargetDb.
	Pool       *pgxpool.Pool    // Access to the target cluster.
	Stores     types.Stagers    // Record incoming json blobs.
	TimeKeeper types.TimeKeeper // Tracks named timestamps.
	Watchers   types.Watchers   // Schema data.
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	sendErr := func(err error) {
		if err == nil {
			http.Error(w, "OK", http.StatusOK)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.WithError(err).WithField("uri", r.RequestURI).Error()
	}

	immediate := false
	if found := r.URL.Query().Get(ImmediateParam); found != "" {
		var err error
		immediate, err = strconv.ParseBool(found)
		if err != nil {
			sendErr(err)
			return
		}
	}

	if ndjson, err := parseNdjsonURL(r.URL.Path); err == nil {
		sendErr(h.ndjson(ctx, ndjson, immediate, r.Body))
		return
	}
	if resolved, err := parseResolvedURL(r.URL.Path); err == nil {
		sendErr(h.resolved(ctx, resolved, immediate))
		return
	}
	if webhook, err := parseWebhookURL(r.URL.Path); err == nil {
		if r.Method == "POST" && r.Header.Get("content-type") == "application/json" {
			sendErr(h.webhook(ctx, webhook, immediate, r.Body))
			return
		}
	}

	http.NotFound(w, r)
}
