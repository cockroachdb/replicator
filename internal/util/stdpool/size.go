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

	"github.com/jackc/pgx/v5/pgxpool"
)

// WithPoolSize sets the nominal size of the database pool.
func WithPoolSize(size int) Option {
	return &withPoolSize{size}
}

type withPoolSize struct {
	size int
}

func (o *withPoolSize) option() {}
func (o *withPoolSize) pgxPoolConfig(_ context.Context, cfg *pgxpool.Config) error {
	cfg.MinConns = int32(o.size)
	cfg.MaxConns = int32(o.size)
	return nil
}
func (o *withPoolSize) sqlDB(_ context.Context, impl *sql.DB) error {
	impl.SetMaxOpenConns(o.size)
	impl.SetMaxIdleConns(o.size)
	return nil
}
