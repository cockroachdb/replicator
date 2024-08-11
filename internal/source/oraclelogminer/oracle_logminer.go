package oraclelogminer

import (
	"github.com/cockroachdb/replicator/internal/util/diag"
	"github.com/cockroachdb/replicator/internal/util/stdlogical"
)

// Need a ProvideConn
type OracleLogminer struct {
	Diagnostics *diag.Diagnostics
	DB          *DB
}

var (
	_ stdlogical.HasDiagnostics = (*OracleLogminer)(nil)
)

// GetDiagnostics implements [stdlogical.HasDiagnostics].
func (l *OracleLogminer) GetDiagnostics() *diag.Diagnostics {
	return l.Diagnostics
}
