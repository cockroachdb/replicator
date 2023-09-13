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
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// WithTransactionTimeout imposes a best-effort attempt at limiting the
// duration of any given database transaction.
func WithTransactionTimeout(d time.Duration) Option { return &withTransactionTimeout{d} }

type withTransactionTimeout struct{ d time.Duration }

func (o *withTransactionTimeout) option() {}
func (o *withTransactionTimeout) pgxPoolConfig(_ context.Context, cfg *pgxpool.Config) error {
	cfg.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] =
		fmt.Sprintf("%d", o.d.Milliseconds())
	return nil
}
