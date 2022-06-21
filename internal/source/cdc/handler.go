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
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// This file contains code repackaged from main.go

const (
	// CASParam is the name of a query parameter that will apply a
	// compare-and-set behavior to processed records. The value of the
	// parameter should be a comma-separated list of column names.
	CASParam = "cas"

	// DeadlineColumnParam is the name of a query parameter that names
	// a column in the incoming data to use in a deadline calculation.
	DeadlineColumnParam = "dln"

	// DeadlineIntervalParam is the name of a query parameter that
	// specifies the deadline time, relative to now() on the target
	// cluster.
	DeadlineIntervalParam = "dli"

	// ImmediateParam is the name of a query parameter that will place a
	// request into "immediate" mode.  In this mode, mutations are
	// written directly to the target table and resolved timestamps are
	// ignored.
	ImmediateParam = "immediate"
)

// Handler is an http.Handler for processing webhook requests
// from a CockroachDB changefeed.
type Handler struct {
	Appliers      types.Appliers      // Update tables within TargetDb.
	Authenticator types.Authenticator // Access checks.
	Pool          *pgxpool.Pool       // Access to the target cluster.
	Resolvers     types.Resolvers     // Record resolved timestamps.
	Stores        types.Stagers       // Record incoming json blobs.
}

// A request is configured by the various parseURL methods in Handler.
type request struct {
	body       io.Reader
	casColumns []ident.Ident
	deadlines  types.Deadlines // Nil if no deadlines set.
	immediate  bool
	leaf       func(ctx context.Context, req *request) error
	target     ident.Schematic
	timestamp  hlc.Time
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

	defer func() {
		if thrown := recover(); thrown != nil {
			err, ok := thrown.(error)
			if !ok {
				err = errors.Errorf("unexpected error: %v", thrown)
			}
			sendErr(err)
		}
	}()

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

	req := &request{}
	switch {
	case h.parseWebhookURL(r.URL, req) == nil:
	case h.parseNdjsonURL(r.URL, req) == nil:
	case h.parseResolvedURL(r.URL, req) == nil:
	default:
		http.NotFound(w, r)
		return
	}

	if !checkAccess(req.target) {
		return
	}

	req.body = r.Body

	query := r.URL.Query()
	if query.Has(CASParam) {
		for _, raw := range query[CASParam] {
			req.casColumns = append(req.casColumns, ident.New(raw))
		}
	}
	// Pair-wise parsing of dln and dli query params.
	if cols, ints := query[DeadlineColumnParam], query[DeadlineIntervalParam]; len(cols)+len(ints) > 0 {
		if len(cols) != len(ints) {
			sendErr(errors.Errorf("mismatch in %s and %s counts",
				DeadlineColumnParam, DeadlineIntervalParam))
			return
		}
		req.deadlines = make(types.Deadlines, len(cols))
		for i, col := range cols {
			d, err := time.ParseDuration(ints[i])
			if err != nil {
				sendErr(err)
				return
			}
			req.deadlines[ident.New(col)] = d
		}
	}
	if found := query.Get(ImmediateParam); found != "" {
		var err error
		req.immediate, err = strconv.ParseBool(found)
		if err != nil {
			sendErr(err)
			return
		}
	}

	sendErr(req.leaf(ctx, req))
}
