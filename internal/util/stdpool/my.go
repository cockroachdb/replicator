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
	"crypto/tls"
	"database/sql"
	sqldriver "database/sql/driver"
	"fmt"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/secure"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// tlsConfigNames assign new names to TLS configuration objects used by the driver.
var tlsConfigNames = onomastic{}

// onomastic is used to create unique names.
type onomastic struct {
	counter atomic.Uint64
}

// newName assign a unique name.
func (o *onomastic) newName(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, o.counter.Add(1))
}

// OpenMySQLAsTarget opens a database connection, returning it as
// a single connection.
func OpenMySQLAsTarget(
	ctx context.Context, connectString string, url *url.URL, options ...Option,
) (*types.TargetPool, func(), error) {
	var tc TestControls
	if err := attachOptions(ctx, &tc, options); err != nil {
		return nil, nil, err
	}
	// Use a unique name for each call of OpenMySQLAsTarget.
	tlsConfigName := tlsConfigNames.newName("mysql_driver")
	return returnOrStop(ctx,
		func(ctx *stopper.Context) (*types.TargetPool, error) {
			tlsConfigs, err := secure.ParseTLSOptions(url)
			if err != nil {
				return nil, err
			}
			var ret *types.TargetPool
			var transportError error
			// Try all possible transport options.
			// The first one that works is the one we will use.
			for _, tlsConfig := range tlsConfigs {
				mysql.DeregisterTLSConfig(tlsConfigName)
				mySQLString, err := getConnString(url, tlsConfigName, tlsConfig)
				if err != nil {
					return nil, errors.WithStack(err)
				}
				err = mysql.RegisterTLSConfig(tlsConfigName, tlsConfig)
				if err != nil {
					return nil, errors.WithStack(err)
				}
				driver := mysql.MySQLDriver{}
				connector, err := driver.OpenConnector(mySQLString)
				if err != nil {
					log.WithError(err).Trace("failed to connect to database server")
					transportError = err
					// Try a different option.
					continue
				}
				ret = &types.TargetPool{
					DB: sql.OpenDB(connector),
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
					// For some errors, we retry.
					if tc.WaitForStartup && isMySQLStartupError(err) {
						log.WithError(err).Info("waiting for database to become ready")
						select {
						case <-ctx.Done():
							return nil, ctx.Err()
						case <-time.After(10 * time.Second):
							goto ping
						}
					}
					transportError = err
					// Try a different option.
					continue
				}
				// Testing that connection is usable.
				if err := ret.QueryRow("SELECT VERSION();").Scan(&ret.Version); err != nil {
					return nil, errors.Wrap(err, "could not query version")
				}
				log.Infof("Version %s.", ret.Version)
				if strings.Contains(ret.Version, "MariaDB") {
					ret.PoolInfo.Product = types.ProductMariaDB
				}
				// If debug is enabled we print sql mode and ssl info.
				if log.IsLevelEnabled(log.DebugLevel) {
					var mode string
					if err := ret.QueryRow("SELECT @@sql_mode").Scan(&mode); err != nil {
						log.Errorf("could not query sql mode %s", err.Error())
					}
					var varName, cipher string
					if err := ret.QueryRow("SHOW STATUS LIKE 'Ssl_cipher';").Scan(&varName, &cipher); err != nil {
						log.Errorf("could not query ssl info %s", err.Error())
					}
					log.Debugf("Mode %s. %s %s", mode, varName, cipher)
					ret.Version = fmt.Sprintf("%s cipher[%s]", ret.Version, cipher)
				}
				if err := attachOptions(ctx, ret.DB, options); err != nil {
					return nil, err
				}
				if err := attachOptions(ctx, &ret.PoolInfo, options); err != nil {
					return nil, err
				}
				// The connection meets the client/server requirements,
				// no need to try other transport options.
				return ret, nil
			}
			// All the options have been exhausted, returning the last error.
			return nil, transportError
		})
}

// TODO (silvano): verify error codes.
func isMySQLStartupError(err error) bool {
	switch err {
	case sqldriver.ErrBadConn:
		return true
	default:
		return false
	}
}

// getConnString returns a driver specific connection strings
// The TLS configuration must be already extracted from the URL parameters
// to determine the list of possible transport connections that the client wants to try.
// This function is only concerned about user, host and path section of the URL.
func getConnString(url *url.URL, tlsConfigName string, config *tls.Config) (string, error) {
	path := "/"
	if url.Path != "" {
		path = url.Path
	}
	baseSQLString := fmt.Sprintf("%s@tcp(%s)%s?%s", url.User.String(), url.Host,
		path, "sql_mode=ansi")
	if config == nil {
		return baseSQLString, nil
	}
	return fmt.Sprintf("%s&tls=%s", baseSQLString, tlsConfigName), nil
}
