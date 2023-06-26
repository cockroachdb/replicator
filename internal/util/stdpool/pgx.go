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

// Package stdpool creates standardized database connection pools.
package stdpool

import (
	"context"
	"database/sql"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// OpenPgxAsConn uses pgx to open a database connection, returning it as
// a single connection.
func OpenPgxAsConn(
	ctx context.Context, connectString string, options ...Option,
) (*pgx.Conn, func(), error) {
	return openPgx(ctx, connectString, options,
		func(ctx *stopper.Context, cfg *pgxpool.Config) (*pgx.Conn, func() error, error) {
			impl, err := pgx.ConnectConfig(ctx, cfg.ConnConfig)
			if err != nil {
				return nil, nil, errors.WithStack(err)
			}
			closeDB := func() error { return impl.Close(context.Background()) }
			return impl, closeDB, nil
		})
}

// OpenPgxAsPool uses pgx to open a database connection, returning it as
// a pool.
func OpenPgxAsPool(
	ctx context.Context, connectString string, options ...Option,
) (*pgxpool.Pool, func(), error) {
	return openPgx(ctx, connectString, options,
		func(ctx *stopper.Context, cfg *pgxpool.Config) (*pgxpool.Pool, func() error, error) {
			impl, err := pgxpool.NewWithConfig(ctx, cfg)
			if err != nil {
				return nil, nil, errors.WithStack(err)
			}
			closeDB := func() error { impl.Close(); return nil }
			return impl, closeDB, nil
		})
}

// OpenPgxAsDB uses pgx to open a database connection, returning it as a
// stdlib pool.
func OpenPgxAsDB(
	ctx context.Context, connectString string, options ...Option,
) (*sql.DB, func(), error) {
	return openPgx(ctx, connectString, options,
		func(ctx *stopper.Context, cfg *pgxpool.Config) (*sql.DB, func() error, error) {
			impl := stdlib.OpenDB(*cfg.ConnConfig)
			closeDB := impl.Close
			return impl, closeDB, nil
		})
}

// openPgx contains the bulk of the behaviors for the various OpenPgx functions.
func openPgx[P any](
	ctx context.Context,
	connectString string,
	options []Option,
	opener func(ctx *stopper.Context, cfg *pgxpool.Config) (P, func() error, error),
) (P, func(), error) {
	return returnOrStop(ctx, func(ctx *stopper.Context) (P, error) {
		cfg, err := pgxpool.ParseConfig(connectString)
		if err != nil {
			return *new(P), errors.Wrapf(err, "could not parse %q", connectString)
		}
		// Identify traffic.
		if _, found := cfg.ConnConfig.RuntimeParams["application_name"]; !found {
			cfg.ConnConfig.RuntimeParams["application_name"] = "cdc-sink"
		}
		if err := attachOptions(ctx, cfg, options); err != nil {
			return *new(P), err
		}

		ret, closeDB, err := opener(ctx, cfg)
		if err != nil {
			return *new(P), err
		}

		// Make sure we clean up the connection.
		ctx.Go(func() error {
			<-ctx.Stopping()
			if err := closeDB(); err != nil {
				log.WithError(err).Warn("error closing database connection")
			}
			return nil
		})

		return ret, attachOptions(ctx, ret, options)
	})
}

// returnOrStop creates a [stopper.Context] from the given context and
// passes the stopper to a callback. If the callback returns an error,
// the stopper will be stopped.
func returnOrStop[T any](
	ctx context.Context, fn func(ctx *stopper.Context) (T, error),
) (T, func(), error) {
	stop := stopper.WithContext(ctx)
	cancel := func() {
		stop.Stop(5 * time.Second)
		if err := stop.Wait(); err != nil {
			log.WithError(err).Warn("error while closing database pool")
		}
	}

	ret, err := fn(stop)
	if err != nil {
		cancel()
		return *new(T), nil, err
	}
	return ret, cancel, nil
}
