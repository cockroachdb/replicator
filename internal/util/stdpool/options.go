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

package stdpool

import (
	"context"
	"database/sql"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	ora "github.com/sijms/go-ora/v2"
)

// Option abstracts over driver-specific configuration.
type Option interface {
	option()
}

// These types are capability interfaces to receive objects that can be
// configured.
type (
	// attachable are all types on which attachOptions can operate.
	attachable interface {
		*ora.OracleConnector | *pgx.Conn | *pgxpool.Config | *pgxpool.Pool | *sql.DB
	}

	oraConnector interface {
		oraConnector(ctx context.Context, conn *ora.OracleConnector) error
	}

	pgxConnOption interface {
		pgxConn(ctx context.Context, conn *pgx.Conn) error
	}

	pgxPoolConfigOption interface {
		pgxPoolConfig(ctx context.Context, cfg *pgxpool.Config) error
	}

	pgxPoolOption interface {
		pgxPool(ctx context.Context, pool *pgxpool.Pool) error
	}

	sqlDBOption interface {
		sqlDB(ctx context.Context, db *sql.DB) error
	}
)

// attachOptions loops over the provided options to compose their
// functionality.
func attachOptions[T attachable](ctx context.Context, target T, options []Option) error {
	// Prepend reasonable defaults.
	options = append([]Option{&withConnectionLifetime{}}, options...)

	switch t := any(target).(type) {
	case *ora.OracleConnector:
		for _, option := range options {
			if x, ok := option.(oraConnector); ok {
				if err := x.oraConnector(ctx, t); err != nil {
					return err
				}
			}
		}

	case *pgx.Conn:
		for _, option := range options {
			if x, ok := option.(pgxConnOption); ok {
				if err := x.pgxConn(ctx, t); err != nil {
					return err
				}
			}
		}

	case *pgxpool.Config:
		for _, option := range options {
			if x, ok := option.(pgxPoolConfigOption); ok {
				if err := x.pgxPoolConfig(ctx, t); err != nil {
					return err
				}
			}
		}

	case *pgxpool.Pool:
		for _, option := range options {
			if x, ok := option.(pgxPoolOption); ok {
				if err := x.pgxPool(ctx, t); err != nil {
					return err
				}
			}
		}

	case *sql.DB:
		for _, option := range options {
			if x, ok := option.(sqlDBOption); ok {
				if err := x.sqlDB(ctx, t); err != nil {
					return err
				}
			}
		}

	default:
		return errors.Errorf("unimplemented: %T", t)
	}

	return nil
}
