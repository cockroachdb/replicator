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
	"database/sql"
	"strconv"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/pkg/errors"
	ora "github.com/sijms/go-ora/v2"
	"github.com/sijms/go-ora/v2/network"
	log "github.com/sirupsen/logrus"
)

func oraErrorCode(err error) (string, bool) {
	if oraErr := (*network.OracleError)(nil); errors.As(err, &oraErr) {
		return strconv.Itoa(oraErr.ErrCode), true
	}
	return "", false
}

func oraErrorDeferrable(err error) bool {
	code, ok := oraErrorCode(err)
	if !ok {
		return false
	}
	switch code {
	case "1": // ORA-0001 unique constraint violated
		// The MERGE that we execute uses read-committed reads, so
		// it's possible for two concurrent merges to attempt to
		// insert the same row.
		return true
	case "60": // ORA-00060: Deadlock detected
		// Our attempt to insert ran into another transaction, possibly
		// from a different cdc-sink instance. This can happen since a
		// MERGE operation reads before it starts writing and the order
		// in which locks are acquired may vary.
		return true
	case "2291": // ORA-02291: integrity constraint
		return true
	default:
		return false
	}
}

func oraErrorRetryable(err error) bool {
	code, ok := oraErrorCode(err)
	if !ok {
		return false
	}
	switch code {
	case "1", // Constraint violation; MERGE reads at read-committed, so concurrent INSERTS are possible.
		"60": // Deadlock detected
		return true
	default:
		return false
	}
}

// OpenOracleAsTarget opens a connection to an Oracle database endpoint and
// return it as a [types.TargetPool].
func OpenOracleAsTarget(
	ctx *stopper.Context, connectString string, options ...Option,
) (*types.TargetPool, error) {
	var tc TestControls
	if err := attachOptions(ctx, &tc, options); err != nil {
		return nil, err
	}

	connector := ora.NewConnector(connectString)

	if err := attachOptions(ctx, connector.(*ora.OracleConnector), options); err != nil {
		return nil, err
	}

	ret := &types.TargetPool{
		DB: sql.OpenDB(connector),
		PoolInfo: types.PoolInfo{
			ConnectionString: connectString,
			Product:          types.ProductOracle,

			ErrCode:      oraErrorCode,
			IsDeferrable: oraErrorDeferrable,
			ShouldRetry:  oraErrorRetryable,
		},
	}
	ctx.Defer(func() {
		// It can take a non-trivial amount of time for this close to
		// happen, since it waits for any in-flight requests to finish
		// as opposed to terminating them early. This slowness
		// complicates test cleanup, since the test can't finish until
		// the pool has closed.
		go func() {
			_ = ret.Close()
		}()
	})

ping:
	if err := ret.Ping(); err != nil {
		if tc.WaitForStartup && isOracleStartupError(err) {
			log.WithError(err).Info("waiting for database to become ready")
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(10 * time.Second):
				goto ping
			}
		}
		return nil, errors.Wrap(err, "could not ping the database")
	}

	if err := ret.QueryRow("SELECT banner FROM V$VERSION").Scan(&ret.Version); err != nil {
		return nil, errors.Wrap(err, "could not query version")
	}

	if err := attachOptions(ctx, ret.DB, options); err != nil {
		return nil, err
	}

	if err := attachOptions(ctx, &ret.PoolInfo, options); err != nil {
		return nil, err
	}
	return ret, nil
}

// These have been enumerated through trial and error.
var oracleStartupErrors = map[int]bool{
	1017:  true, // Invalid username/password
	1109:  true, // Database not open
	12514: true, // TNS listener doesn't know about the service
	28000: true, // The account is locked
}

func isOracleStartupError(err error) bool {
	var oErr *network.OracleError
	if !errors.As(err, &oErr) {
		return false
	}
	return oracleStartupErrors[oErr.ErrCode]
}
