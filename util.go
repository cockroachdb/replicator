// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

// This code is taken from the Cacheroach project.

import (
	"context"

	"github.com/jackc/pgconn"
	"github.com/pkg/errors"
)

// Marker is a settable flag.
type Marker bool

// Mark sets the flag.
func (m *Marker) Mark() {
	*m = true
}

// Marked returns the flag status.
func (m *Marker) Marked() bool {
	return bool(*m)
}

// Retry is a convenience wrapper to automatically retry idempotent
// database operations that experience a transaction or or connection
// failure. The provided callback must be entirely idempotent, with
// no observable side-effects during its execution.
func Retry(ctx context.Context, idempotent func(context.Context) error) error {
	return RetryLoop(ctx, func(ctx context.Context, _ *Marker) error {
		return idempotent(ctx)
	})
}

// RetryLoop is a convenience wrapper to automatically retry idempotent
// database operations that experience a transaction or or connection
// failure. The provided callback may indicate that it has started
// generating observable effects (e.g. sending result data) by calling
// its second parameter to disable the retry behavior.
func RetryLoop(ctx context.Context, fn func(ctx context.Context, sideEffect *Marker) error) error {
	var sideEffect Marker
	for {
		err := fn(ctx, &sideEffect)
		if err == nil || sideEffect.Marked() {
			return err
		}

		if pgErr := (*pgconn.PgError)(nil); errors.As(err, &pgErr) {
			switch pgErr.Code {
			case "40001": // Serialization Failure
			case "40003": // Statement Completion Unknown
			case "08003": // Connection Does Not Exist
			case "08006": // Connection Failure
			default:
				return err
			}
		} else {
			return err
		}
	}
}
