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
	errors2 "errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/sinktest"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/secure"
	"github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// See also:
// https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html
func myErrCode(err error) (string, bool) {
	if myErr := (*mysql.MySQLError)(nil); errors.As(err, &myErr) {
		return strconv.Itoa(int(myErr.Number)), true
	}
	return "", false
}

func myErrDeferrable(err error) bool {
	code, ok := myErrCode(err)
	if !ok {
		return false
	}
	switch code {
	case "1451":
		// Cannot add or update a parent row: a foreign key constraint fails
		return true
	case "1452":
		// Cannot add or update a child row: a foreign key constraint fails
		return true
	default:
		return false
	}
}

func myErrRetryable(err error) bool {
	code, ok := myErrCode(err)
	if !ok {
		return false
	}
	// Deadlock detected due to concurrent modification.
	return code == "40001"
}

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
	ctx *stopper.Context,
	connectString string,
	backup *Backup,
	breakers *sinktest.Breakers,
	options ...Option,
) (*types.TargetPool, error) {
	var tc TestControls
	if err := attachOptions(ctx, &tc, options); err != nil {
		return nil, err
	}

	connector, err := newTLSFallbackConnector(connectString, &breakers.TargetConnectionFails)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ret := &types.TargetPool{
		DB: sql.OpenDB(connector),
		PoolInfo: types.PoolInfo{
			ConnectionString: connectString,
			Product:          types.ProductMySQL,

			ErrCode:      myErrCode,
			IsDeferrable: myErrDeferrable,
			ShouldRetry:  myErrRetryable,
		},
	}

	if tc.WaitForStartup {
		if err := awaitMySQLReady(ctx, ret); err != nil {
			return nil, err
		}
		if ctx.IsStopping() {
			return nil, ctx.Err()
		}
	}

	// Testing that connection is usable.
	if err := ret.QueryRow("SELECT VERSION();").Scan(&ret.Version); err != nil {
		queryErr := errors.Wrap(err, "could not query version")
		ver, err := backup.Load(ctx, connectString)
		if err != nil {
			return nil, errors2.Join(queryErr, errors.Wrap(err, "could not load version from staging"))
		}
		if ver == "" {
			return nil, fmt.Errorf("empty version loaded from staging")
		}
		ret.Version = ver
	} else if err := backup.Store(ctx, connectString, ret.Version); err != nil {
		return nil, errors.Wrap(err, "could not store version to staging")
	}
	log.Infof("Version %s.", ret.Version)
	if strings.Contains(ret.Version, "MariaDB") {
		ret.PoolInfo.Product = types.ProductMariaDB
	}
	if err := setTableHint(ret.Info()); err != nil {
		return nil, err
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

	return ret, nil
}

type mySQLDriver struct{}

func (m *mySQLDriver) Open(name string) (sqldriver.Conn, error) {
	u, err := url.Parse(name)
	if err != nil {
		return nil, err
	}
	tlsConfigs, err := secure.ParseTLSOptions(u)
	if err != nil {
		return nil, err
	}

	ctor := tlsFallbackConnector{
		tlsConfigs: tlsConfigs,
	}
	return ctor.Connect(context.Background())
}

type tlsFallbackConnector struct {
	tlsConfigs      []*tls.Config
	connectionURL   *url.URL
	delegate        sqldriver.Connector
	failConnections *atomic.Bool
}

func (t *tlsFallbackConnector) findDelegate() (sqldriver.Connector, error) {
	if t.delegate != nil {
		return t.delegate, nil
	}

	// Use a unique name for each call of OpenMySQLAsTarget.
	tlsConfigName := tlsConfigNames.newName("mysql_driver")

	var lastErr error
	// Try all possible transport options.
	// The first one that works is the one we will use.
	for _, tlsConfig := range t.tlsConfigs {
		mysql.DeregisterTLSConfig(tlsConfigName)
		mySQLString, err := getConnString(t.connectionURL, tlsConfigName, tlsConfig)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		err = mysql.RegisterTLSConfig(tlsConfigName, tlsConfig)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		cfg, err := mysql.ParseDSN(mySQLString)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		// This impacts the use of db.Query() and friends, but not
		// prepared statements. We set this as a workaround for MariaDB
		// queries where the parameter types in prepared statements
		// don't line up with MySQL. What this option does is to replace
		// the ? markers with the literal values, rather than doing a
		// prepare, bind, exec.
		cfg.InterpolateParams = true
		mySQLConnector, err := mysql.NewConnector(cfg)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		myDB := sql.OpenDB(mySQLConnector)
		defer myDB.Close()
		if err := myDB.Ping(); err != nil {
			lastErr = err
		} else {
			return mySQLConnector, nil
		}
	}

	// Nothing worked; return the last error
	return nil, lastErr
}

func (t *tlsFallbackConnector) Connect(ctx context.Context) (sqldriver.Conn, error) {
	if t.failConnections.Load() {
		return nil, fmt.Errorf("testing connection failure")
	}
	if t.delegate == nil {
		delegate, err := t.findDelegate()
		if err != nil {
			return nil, err
		}
		if delegate == nil {
			// This shouldn't happen; we should either find a delegate, or error
			return nil, fmt.Errorf("could not find MySQL connector delegate")
		}
		t.delegate = delegate
	}
	return t.delegate.Connect(ctx)
}

func (t *tlsFallbackConnector) Driver() sqldriver.Driver {
	return &mySQLDriver{}
}

func newTLSFallbackConnector(
	connStr string, failConnections *atomic.Bool,
) (sqldriver.Connector, error) {
	u, err := url.Parse(connStr)
	if err != nil {
		return nil, err
	}

	tlsConfigs, err := secure.ParseTLSOptions(u)
	if err != nil {
		return nil, err
	}

	return &tlsFallbackConnector{
		tlsConfigs:      tlsConfigs,
		connectionURL:   u,
		failConnections: failConnections,
	}, nil
}

func awaitMySQLReady(ctx *stopper.Context, db *types.TargetPool) error {
	for {
		err := db.Ping()
		if err == nil || !isMySQLStartupError(err) {
			return err
		}

		// We have a startup error
		log.WithError(err).Info("waiting for database to become ready")

		select {
		case <-ctx.Stopping():
			return nil
		case <-time.After(10 * time.Second):
		}
	}
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
