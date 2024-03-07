// Copyright 2024 The Cockroach Authors
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

package sequtil

import (
	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pkg/errors"
	"github.com/sijms/go-ora/v2/network"
)

// IsDeferrableError returns true if the error represents an error
// that's likely to go away on its own in the future (e.g. FK
// constraints). These are also errors that we're ok with reducing log
// levels for.
//
// https://github.com/cockroachdb/cdc-sink/issues/688
func IsDeferrableError(err error) bool {
	if err == nil {
		return false
	}
	if pgErr := (*pgconn.PgError)(nil); errors.As(err, &pgErr) {
		return pgErr.Code == "23503" // foreign_key_violation
	}
	if myErr := (*mysql.MySQLError)(nil); errors.As(err, &myErr) {
		// Cannot add or update a child row: a foreign key constraint fails
		return myErr.Number == 1452
	}
	if oraErr := (*network.OracleError)(nil); errors.As(err, &oraErr) {
		switch oraErr.ErrCode {
		case 1: // ORA-0001 unique constraint violated
			// The MERGE that we execute uses read-committed reads, so
			// it's possible for two concurrent merges to attempt to
			// insert the same row.
			return true
		case 60: // ORA-00060: Deadlock detected
		// Our attempt to insert ran into another transaction, possibly
		// from a different cdc-sink instance. This can happen since a
		// MERGE operation reads before it starts writing and the order
		// in which locks are acquired may vary.
		case 2291: // ORA-02291: integrity constraint
			return true
		}
	}
	return false
}
