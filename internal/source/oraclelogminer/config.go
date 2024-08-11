package oraclelogminer

import (
	"github.com/cockroachdb/replicator/internal/script"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/sinkprod"
	"github.com/cockroachdb/replicator/internal/target/dlq"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

type Config struct {
	DLQ       dlq.Config
	Script    script.Config
	Sequencer sequencer.Config
	Staging   sinkprod.StagingConfig
	Target    sinkprod.TargetConfig

	// Connection string for the source db.
	SourceConn string
	// The SQL schema in the target cluster to write into. This value is
	// optional if a userscript dispatch function is present.
	TargetSchema ident.Schema

	SCN string
}

// Bind adds flags to the set.
func (c *Config) Bind(f *pflag.FlagSet) {
	c.DLQ.Bind(f)
	c.Script.Bind(f)
	c.Sequencer.Bind(f)
	c.Staging.Bind(f)
	c.Target.Bind(f)

	f.StringVar(&c.SourceConn, "sourceConn", "", "the source database's connection string")

	f.Var(ident.NewSchemaFlag(&c.TargetSchema), "targetSchema",
		"the SQL database schema in the target cluster to update")

	f.StringVar(&c.SCN, "scn", "", "starting SCN for logMiner")
}

// Preflight updates the configuration with sane defaults or returns an
// error if there are missing options for which a default cannot be
// provided.
func (c *Config) Preflight() error {
	if err := c.DLQ.Preflight(); err != nil {
		return err
	}
	if err := c.Script.Preflight(); err != nil {
		return err
	}
	if err := c.Sequencer.Preflight(); err != nil {
		return err
	}
	if err := c.Staging.Preflight(); err != nil {
		return err
	}
	if err := c.Target.Preflight(); err != nil {
		return err
	}

	// We can disable idempotent tracking in the sequencer stack
	// since the logical stream is idempotent.
	c.Sequencer.IdempotentSource = true

	if c.SourceConn == "" {
		return errors.New("no source connection was configured")
	}
	if c.TargetSchema.Empty() {
		return errors.New("no target schema specified")
	}
	return nil
}
