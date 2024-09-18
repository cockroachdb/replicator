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

//go:build cgo && (target_oracle || target_all || source_oracle || source_all)

package stdpool

import (
	"database/sql"
	"os"
	"strconv"
	"time"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/godror/godror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

func oraErrorCode(err error) (string, bool) {
	if oraErr := (*godror.OraErr)(nil); errors.As(err, &oraErr) {
		return strconv.Itoa(oraErr.Code()), true
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
		// from a different Replicator instance. This can happen since a
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

// OCIPathEnvVar is the path where the Oracle Instant Client library is stored.
const OCIPathEnvVar = `OIC_LIBRARY_PATH`

func openOracle(ctx *stopper.Context, connectString string, options ...Option) (*sql.DB, error) {
	var tc TestControls
	if err := attachOptions(ctx, &tc, options); err != nil {
		return nil, err
	}

	params, err := godror.ParseDSN(connectString)
	if err != nil {
		return nil, err
	}
	// Ensure consistent behavior, where CHAR(1) => CHAR(1 CHAR).
	params.AlterSession = [][2]string{
		{"NLS_LENGTH_SEMANTICS", "CHAR"},
	}
	// Use go's pool, instead of the C library's pool.
	params.StandaloneConnection = true
	// If unset, the driver would otherwise use the local system timezone.
	if params.Timezone == nil {
		params.Timezone = time.UTC
	}

	if ociLibPath := os.Getenv(OCIPathEnvVar); ociLibPath != "" {
		params.LibDir = ociLibPath
	}

	db := sql.OpenDB(godror.NewConnector(params))

ping:
	if err := db.Ping(); err != nil {
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

	return db, nil
}

// OpenOracleAsTarget opens a connection to an Oracle database endpoint and
// return it as a [types.SourcePool].
func OpenOracleAsSource(
	ctx *stopper.Context, connectString string, options ...Option,
) (*types.SourcePool, error) {

	db, err := openOracle(ctx, connectString, options...)
	if err != nil {
		return nil, err
	}

	ret := &types.SourcePool{
		DB: db,
		PoolInfo: types.PoolInfo{
			ConnectionString: connectString,
			Product:          types.ProductOracle,

			ErrCode:      oraErrorCode,
			IsDeferrable: oraErrorDeferrable,
			ShouldRetry:  oraErrorRetryable,
		},
	}
	ctx.Defer(func() { _ = ret.Close() })

	if err := logMnrEnabledCheck(ctx, ret); err != nil {
		return nil, errors.Wrapf(err, "failed to start replicator for oracle source")
	}

	return ret, nil
}

// OpenOracleAsTarget opens a connection to an Oracle database endpoint and
// return it as a [types.TargetPool].
func OpenOracleAsTarget(
	ctx *stopper.Context, connectString string, options ...Option,
) (*types.TargetPool, error) {
	db, err := openOracle(ctx, connectString, options...)
	if err != nil {
		return nil, err
	}

	ret := &types.TargetPool{
		DB: db,
		PoolInfo: types.PoolInfo{
			ConnectionString: connectString,
			Product:          types.ProductOracle,

			ErrCode:      oraErrorCode,
			IsDeferrable: oraErrorDeferrable,
			ShouldRetry:  oraErrorRetryable,
		},
	}
	ctx.Defer(func() { _ = ret.Close() })

	if err := ret.QueryRow("SELECT banner FROM V$VERSION").Scan(&ret.Version); err != nil {
		return nil, errors.Wrap(err, "could not query version")
	}
	if err := setTableHint(ret.Info()); err != nil {
		return nil, err
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
var oracleStartupErrors = map[string]bool{
	"1017":  true, // Invalid username/password
	"1109":  true, // Database not open
	"12514": true, // TNS listener doesn't know about the service
	"28000": true, // The account is locked
}

func isOracleStartupError(err error) bool {
	code, ok := oraErrorCode(err)
	if !ok {
		return false
	}
	return oracleStartupErrors[code]
}

const (
	logModeCheckStmt = `SELECT log_mode FROM v$database`
	archiveLog       = `ARCHIVELOG`
)
const (
	supplementalLogDataStmt = `SELECT SUPPLEMENTAL_LOG_DATA_MIN, SUPPLEMENTAL_LOG_DATA_PK FROM V$DATABASE`
)

// logMnrEnabledCheck checks if essential settings required by LogMiner are enabled.
func logMnrEnabledCheck(ctx *stopper.Context, db *types.SourcePool) error {
	var logModeStr sql.NullString
	if err := db.QueryRowContext(ctx, logModeCheckStmt).Scan(&logModeStr); err != nil {
		return errors.Wrapf(err, "failed to query the log mode")
	}

	if !logModeStr.Valid || logModeStr.String != archiveLog {
		return errors.New("archive log is not enabled")
	}

	var supplementLogMinStr, supplementLogPKStr sql.NullString

	if err := db.QueryRowContext(ctx, supplementalLogDataStmt).Scan(&supplementLogMinStr, &supplementLogPKStr); err != nil {
		return errors.Wrapf(err, "failed to check if supplement log has been enabled")
	}

	if !supplementLogMinStr.Valid || supplementLogMinStr.String != "YES" {
		return errors.WithMessage(errors.New("supplemental log data is not enabled"), "please run ALTER DATABASE ADD SUPPLEMENTAL LOG DATA")
	}

	if !supplementLogPKStr.Valid || supplementLogPKStr.String != "YES" {
		return errors.WithMessage(errors.New("supplemental log data with PK is not enabled"), "please run ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS")
	}
	log.Info("logMiner is confirmed enabled")

	return nil
}
