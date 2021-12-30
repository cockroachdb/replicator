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
	"strings"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
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
	Appliers      types.Appliers      // Update tables within TargetDb.
	Authenticator types.Authenticator // Access checks.
	Pool          *pgxpool.Pool       // Access to the target cluster.
	Stores        types.Stagers       // Record incoming json blobs.
	Swapper       types.TimeKeeper    // Tracks named timestamps.
	Watchers      types.Watchers      // Schema data.
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

	// checkAccess returns true if the request should continue.
	checkAccess := func(target ident.Schematic) bool {
		var token string
		if raw := r.Header.Get("Authorization"); raw != "" {
			if strings.HasPrefix(raw, "Bearer ") {
				token = raw[7:]
			}
		} else if token = r.URL.Query().Get("access_token"); token != "" {
			// Delete the token query parameter to prevent logging later
			// on. This doesn't take care of issues with L7
			// loadbalancers, but this param is used by other
			// OAuth-style implementations and will hopefully be
			// discarded without logging.
			values := r.URL.Query()
			values.Del("access_token")
			r.URL.RawQuery = values.Encode()
		}
		// It's OK if token is empty here, we might be using a trivial
		// Authenticator.
		ok, err := h.Authenticator.Check(ctx, target.AsSchema(), token)
		if err != nil {
			sendErr(err)
			return false
		}
		if ok {
			return true
		}
		http.Error(w, "missing or invalid access token", http.StatusUnauthorized)
		return false
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
		if checkAccess(ndjson.target) {
			sendErr(h.ndjson(ctx, ndjson, immediate, r.Body))
		}
		return
	}
	if resolved, err := parseResolvedURL(r.URL.Path); err == nil {
		if checkAccess(resolved.target) {
			sendErr(h.resolved(ctx, resolved, immediate))
		}
		return
	}
	if webhook, err := parseWebhookURL(r.URL.Path); err == nil {
		if checkAccess(webhook.target) {
			sendErr(h.webhook(ctx, webhook, immediate, r.Body))
		}
		return
	}

	http.NotFound(w, r)
}
