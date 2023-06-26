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
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// WithConnectionLifetime configures a nominal connection pool size.
func WithConnectionLifetime(lifetime time.Duration) Option {
	return &withConnectionLifetime{lifetime}
}

type withConnectionLifetime struct {
	lifetime time.Duration
}

func (o *withConnectionLifetime) option() {}
func (o *withConnectionLifetime) pgxPoolConfig(_ context.Context, cfg *pgxpool.Config) error {
	cfg.MaxConnLifetime = o.lifetime
	if cfg.MaxConnLifetimeJitter == 0 {
		cfg.MaxConnLifetimeJitter = time.Minute
	}
	return nil
}
func (o *withConnectionLifetime) sqlDB(_ context.Context, db *sql.DB) error {
	db.SetConnMaxLifetime(o.lifetime)
	return nil
}
