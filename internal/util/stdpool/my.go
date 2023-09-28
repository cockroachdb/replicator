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
	sqldriver "database/sql/driver"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	_ "github.com/go-mysql-org/go-mysql/driver" // register driver
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// OpenMySQLAsTarget opens a database connection, returning it as
// a single connection.
func OpenMySQLAsTarget(
	ctx context.Context, connectString string, options ...Option,
) (*types.TargetPool, func(), error) {
	var tc TestControls
	if err := attachOptions(ctx, &tc, options); err != nil {
		return nil, nil, err
	}

	return returnOrStop(ctx, func(ctx *stopper.Context) (*types.TargetPool, error) {
		log.Info(connectString)

		connector, err := sql.Open("mysql", connectString)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		ret := &types.TargetPool{
			DB: connector,
			PoolInfo: types.PoolInfo{
				ConnectionString: connectString,
				Product:          types.ProductMySQL,
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
			if tc.WaitForStartup && isMySQLStartupError(err) {
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

		if err := ret.QueryRow("SELECT VERSION();").Scan(&ret.Version); err != nil {
			return nil, errors.Wrap(err, "could not query version")
		}
		log.Infof("Version %s", ret.Version)
		// Setting sql_mode so we can use quotes (") for Ident.
		if _, err := ret.Exec("SET SESSION sql_mode = 'ansi'"); err != nil {
			return nil, errors.Wrap(err, "could not set sql_mode to ansi")
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

// TODO (silvano): verify error codes
func isMySQLStartupError(err error) bool {
	switch err {
	case sqldriver.ErrBadConn:
		return true
	default:
		return false
	}
}
