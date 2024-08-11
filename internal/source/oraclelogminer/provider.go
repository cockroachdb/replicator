package oraclelogminer

import (
	"database/sql"
	"os"
	"time"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/godror/godror"
	"github.com/google/wire"
	"github.com/pkg/errors"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideDB,
)

const (
	logModeCheckStmt = `select log_mode from v$database`
	archiveLog       = `ARCHIVELOG`
)
const supplementalLogDataStmt = `select supplemental_log_data_min, supplemental_log_data_pk from v$database`

const OCIPathEnvVar = `OIC_LIBRARY_PATH`

func ProvideDB(ctx *stopper.Context, config *Config) (*DB, error) {
	if err := config.Preflight(); err != nil {
		return nil, err
	}
	params, err := godror.ParseDSN(config.SourceConn)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse the source connection string for parameters")
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

	connector := godror.NewConnector(params)

	db := sql.OpenDB(connector)

	if err := db.Ping(); err != nil {
		return nil, errors.Wrapf(err, "failed pinging oracle db")
	}

	if err := logMnrEnabledCheck(ctx, db); err != nil {
		return nil, errors.Wrapf(err, "failed to start replicator for oracle source")
	}

	res := &DB{ConnStr: config.SourceConn, DB: db, UserName: params.Username}
	if config.SCN != "" {
		res.SCN = config.SCN
	}

	return res, nil
}

func logMnrEnabledCheck(ctx *stopper.Context, db *sql.DB) error {
	var logModeStr sql.NullString
	if err := db.QueryRowContext(ctx, logModeCheckStmt).Scan(&logModeStr); err != nil {
		return errors.Wrapf(err, "failed to query the log mode")
	}

	if !logModeStr.Valid || logModeStr.String != archiveLog {
		return errors.New("archive log is not enabled")
	}

	var supplementLogStr sql.NullString

	if err := db.QueryRowContext(ctx, supplementalLogDataStmt).Scan(&supplementLogStr); err != nil {
		return errors.Wrapf(err, "failed to check if supplement log has been enabled")
	}

	if !supplementLogStr.Valid || supplementLogStr.String != "YES" {
		return errors.WithMessage(errors.New("supplemental log data is not enabled"), "please run ALTER DATABASE ADD SUPPLEMENTAL LOG DATA")
	}

	return nil
}
