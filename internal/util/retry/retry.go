// Copyright 2023 The Cockroach Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

// Package retry contains utility code for retrying database transactions.
package retry

// This code is taken from the Cacheroach project.

import (
	"context"
	"fmt"
	"strconv"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pkg/errors"
	"github.com/sijms/go-ora/v2/network"
)

// Marker is a settable flag.
type Marker bool

// Mark sets the flag.
func (m *Marker) Mark() { *m = true }

// Marked returns the flag status.
func (m *Marker) Marked() bool { return bool(*m) }

// Execute is a wrapper around Retry that can be used for sql
// queries that don't have any return values.
func Execute[P types.AnyPool](ctx context.Context, db P, query string, args ...any) error {
	return Retry(ctx, func(ctx context.Context) error {
		var err error
		switch t := any(db).(type) {
		case *types.SourcePool:
			_, err = t.ExecContext(ctx, query, args...)
		case *types.StagingPool:
			_, err = t.Exec(ctx, query, args...)
		case *types.TargetPool:
			_, err = t.ExecContext(ctx, query, args...)
		default:
			err = fmt.Errorf("unimplemented %T", t)
		}
		return err
	})
}

// Retry is a convenience wrapper to automatically retry idempotent
// database operations that experience a transaction or or connection
// failure. The provided callback must be entirely idempotent, with
// no observable side-effects during its execution.
func Retry(ctx context.Context, idempotent func(context.Context) error) error {
	return Loop(ctx, func(ctx context.Context, _ *Marker) error {
		return idempotent(ctx)
	})
}

// inLoop is a key used by Loop to detect reentrant behavior.
type inLoop struct{}

// Loop is a convenience wrapper to automatically retry idempotent
// database operations that experience a transaction or a connection
// failure. The provided callback may indicate that it has started
// generating observable effects (e.g. sending result data) by calling
// its second parameter to disable the retry behavior.
//
// If Loop is called in a reentrant fashion, the retry behavior will be
// suppressed within an inner loop, allowing the retryable error to
// percolate into the outer loop.
func Loop(ctx context.Context, fn func(ctx context.Context, sideEffect *Marker) error) error {
	const maxAttempts = 10
	if outerMarker, ok := ctx.Value(inLoop{}).(*Marker); ok {
		return fn(ctx, outerMarker)
	}

	var sideEffect Marker
	ctx = context.WithValue(ctx, inLoop{}, &sideEffect)
	actionsCount.Inc()
	attempt := 0
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
				abortedCount.WithLabelValues(pgErr.Code).Inc()
				return err
			}
			retryCount.WithLabelValues(pgErr.Code).Inc()
		} else if oraErr := (*network.OracleError)(nil); errors.As(err, &oraErr) {
			switch oraErr.ErrCode {
			case 1: // Constraint violation; MERGE reads at read-committed, so concurrent INSERTS are possible.
			case 60: // Deadlock detected
			default:
				abortedCount.WithLabelValues(strconv.Itoa(oraErr.ErrCode))
				return err
			}
		} else {
			return err
		}
		attempt++
		if attempt >= maxAttempts {
			return errors.Wrapf(err, "maximum number of retries (%d) exceeded", maxAttempts)
		}
	}
}
