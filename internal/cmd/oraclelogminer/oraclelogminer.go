package oraclelogminer

import (
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/source/oraclelogminer"
	"github.com/cockroachdb/replicator/internal/util/stdlogical"
	"github.com/spf13/cobra"
)

// Command returns the pglogical subcommand.
func Command() *cobra.Command {
	cfg := &oraclelogminer.Config{}
	return stdlogical.New(&stdlogical.Template{
		Config: cfg,
		Short:  "start an oracle logMiner replication feed",
		Start: func(ctx *stopper.Context, cmd *cobra.Command) (any, error) {
			return oraclelogminer.Start(ctx, cfg)
		},
		Use: "oraclelogminer",
	})
}
