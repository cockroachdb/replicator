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

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/pkg/errors"
	ora "github.com/sijms/go-ora/v2"
	"github.com/sijms/go-ora/v2/network"
	log "github.com/sirupsen/logrus"
)

// OpenOracleAsTarget opens a connection to an Oracle database endpoint and
// return it as a [types.TargetPool].
func OpenOracleAsTarget(
	ctx context.Context, connectString string, options ...Option,
) (*types.TargetPool, func(), error) {
	var tc TestControls
	if err := attachOptions(ctx, &tc, options); err != nil {
		return nil, nil, err
	}

	return returnOrStop(ctx, func(ctx *stopper.Context) (*types.TargetPool, error) {
		unconfigured, err := sql.Open("oracle", "")
		if err != nil {
			return nil, errors.WithStack(err)
		}

		connector, err := unconfigured.Driver().(*ora.OracleDriver).OpenConnector(connectString)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		if err := attachOptions(ctx, connector.(*ora.OracleConnector), options); err != nil {
			return nil, err
		}

		ret := &types.TargetPool{
			DB: sql.OpenDB(connector),
			PoolInfo: types.PoolInfo{
				ConnectionString: connectString,
				Product:          types.ProductOracle,
			},
		}

		ctx.Go(func() error {
			<-ctx.Stopping()
			if err := ret.Close(); err != nil {
				log.WithError(errors.WithStack(err)).Warn("could not close database connection")
			}
			return nil
		})

	ping:
		if err := ret.Ping(); err != nil {
			if tc.WaitForStartup && isStartupError(err) {
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
	})
}

// These have been enumerated through trial and error.
var oracleStartupErrors = map[int]bool{
	1017:  true, // Invalid username/password
	1109:  true, // Database not open
	12514: true, // TNS listener doesn't know about the service
	28000: true, // The account is locked
}

func isStartupError(err error) bool {
	var oErr *network.OracleError
	if !errors.As(err, &oErr) {
		return false
	}
	return oracleStartupErrors[oErr.ErrCode]
}
