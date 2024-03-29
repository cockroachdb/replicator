// Code generated by Wire. DO NOT EDIT.

//go:generate go run -mod=mod github.com/google/wire/cmd/wire
//go:build !wireinject
// +build !wireinject

package kafka

import (
	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/besteffort"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/chaos"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/core"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/immediate"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/scheduler"
	script2 "github.com/cockroachdb/cdc-sink/internal/sequencer/script"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/switcher"
	"github.com/cockroachdb/cdc-sink/internal/sinkprod"
	"github.com/cockroachdb/cdc-sink/internal/staging/leases"
	"github.com/cockroachdb/cdc-sink/internal/staging/memo"
	"github.com/cockroachdb/cdc-sink/internal/staging/stage"
	"github.com/cockroachdb/cdc-sink/internal/target/apply"
	"github.com/cockroachdb/cdc-sink/internal/target/dlq"
	"github.com/cockroachdb/cdc-sink/internal/target/schemawatch"
	"github.com/cockroachdb/cdc-sink/internal/util/applycfg"
	"github.com/cockroachdb/cdc-sink/internal/util/diag"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
)

// Injectors from injector.go:

// Start creates a Kafka logical replication loop using the
// provided configuration.
func Start(ctx *stopper.Context, config *Config) (*Kafka, error) {
	diagnostics := diag.New(ctx)
	configs, err := applycfg.ProvideConfigs(diagnostics)
	if err != nil {
		return nil, err
	}
	scriptConfig := &config.Script
	loader, err := script.ProvideLoader(ctx, configs, scriptConfig, diagnostics)
	if err != nil {
		return nil, err
	}
	eagerConfig := ProvideEagerConfig(config, loader)
	targetConfig := &eagerConfig.Target
	targetPool, err := sinkprod.ProvideTargetPool(ctx, targetConfig, diagnostics)
	if err != nil {
		return nil, err
	}
	targetStatements, err := sinkprod.ProvideStatementCache(ctx, targetConfig, targetPool, diagnostics)
	if err != nil {
		return nil, err
	}
	dlqConfig := &eagerConfig.DLQ
	watchers, err := schemawatch.ProvideFactory(ctx, targetPool, diagnostics)
	if err != nil {
		return nil, err
	}
	dlQs := dlq.ProvideDLQs(dlqConfig, targetPool, watchers)
	acceptor, err := apply.ProvideAcceptor(ctx, targetStatements, configs, diagnostics, dlQs, targetPool, watchers)
	if err != nil {
		return nil, err
	}
	sequencerConfig := &eagerConfig.Sequencer
	stagingConfig := &eagerConfig.Staging
	stagingPool, err := sinkprod.ProvideStagingPool(ctx, stagingConfig, diagnostics, targetConfig)
	if err != nil {
		return nil, err
	}
	stagingSchema, err := sinkprod.ProvideStagingDB(stagingConfig)
	if err != nil {
		return nil, err
	}
	typesLeases, err := leases.ProvideLeases(ctx, stagingPool, stagingSchema)
	if err != nil {
		return nil, err
	}
	schedulerScheduler, err := scheduler.ProvideScheduler(ctx, sequencerConfig)
	if err != nil {
		return nil, err
	}
	stagers := stage.ProvideFactory(stagingPool, stagingSchema, ctx)
	bestEffort := besteffort.ProvideBestEffort(sequencerConfig, typesLeases, schedulerScheduler, stagingPool, stagers, targetPool, watchers)
	coreCore := core.ProvideCore(sequencerConfig, typesLeases, schedulerScheduler, stagers, stagingPool, targetPool)
	immediateImmediate := immediate.ProvideImmediate(targetPool)
	sequencer := script2.ProvideSequencer(loader, targetPool, watchers)
	switcherSwitcher := switcher.ProvideSequencer(bestEffort, coreCore, diagnostics, immediateImmediate, sequencer, stagingPool, targetPool)
	chaosChaos := &chaos.Chaos{
		Config: sequencerConfig,
	}
	memoMemo, err := memo.ProvideMemo(ctx, stagingPool, stagingSchema)
	if err != nil {
		return nil, err
	}
	conn, err := ProvideConn(ctx, acceptor, switcherSwitcher, chaosChaos, config, memoMemo, stagingPool, targetPool, watchers)
	if err != nil {
		return nil, err
	}
	kafka := &Kafka{
		Conn:        conn,
		Diagnostics: diagnostics,
	}
	return kafka, nil
}