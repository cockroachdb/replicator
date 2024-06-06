// Copyright 2024 The Cockroach Authors
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

package workload

import (
	"testing"
	"time"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/sinkprod"
	"github.com/cockroachdb/replicator/internal/sinktest/all"
	"github.com/cockroachdb/replicator/internal/sinktest/base"
	"github.com/cockroachdb/replicator/internal/source/cdc/server"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/stdserver"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// This is a white-box test that creates a server and executes the
// workload against it for a few seconds.
func TestWorkload(t *testing.T) {
	t.Parallel()
	const testTime = 5 * time.Second
	r := require.New(t)

	fixture, err := all.NewFixture(t)
	r.NoError(err)
	ctx := fixture.Context

	serverCfg := &server.Config{
		HTTP: stdserver.Config{
			BindAddr:           "127.0.0.1:0",
			GenerateSelfSigned: true,
		},
		Staging: sinkprod.StagingConfig{
			CommonConfig: sinkprod.CommonConfig{
				Conn: fixture.StagingPool.ConnectionString,
			},
			CreateSchema: true,
			Schema:       fixture.StagingDB.Schema(),
		},
		Target: sinkprod.TargetConfig{
			CommonConfig: sinkprod.CommonConfig{
				Conn: fixture.TargetPool.ConnectionString,
			},
		},
	}

	workload, group, err := fixture.NewWorkload(ctx, &all.WorkloadConfig{})
	r.NoError(err)

	cfg := &clientConfig{
		childTable:   workload.Child.Name(),
		parentTable:  workload.Parent.Name(),
		targetSchema: fixture.TargetSchema.Schema(),
	}

	svr, err := cfg.newServer(ctx, serverCfg)
	r.NoError(err)

	r.NoError(cfg.initURL(svr.GetListener()))

	r.NoError(cfg.generateJWT(ctx, svr))

	// Create a runner, but inject the generator from above. This will
	// allow us to validate the behavior later.
	runner, err := cfg.newRunner(ctx, workload.GeneratorBase)
	r.NoError(err)

	// Create a nested stopper, so we can run the workload generator
	// for a period of time.
	runnerCtx := stopper.WithContext(ctx)
	runnerCtx.Go(func(runnerCtx *stopper.Context) error {
		return runner.Run(runnerCtx)
	})

	// Wait for a bit.
	select {
	case <-time.After(testTime):
		log.Info("waiting for runner context to finish")
		runnerCtx.Stop(time.Second)
		r.NoError(runnerCtx.Wait())
	case <-ctx.Stopping():
	}

	var resolvedRange notify.Var[hlc.Range]
	_, err = svr.Checkpoints.Start(ctx, group, &resolvedRange)
	r.NoError(err)
	for {
		progress, changed := resolvedRange.Get()
		if hlc.Compare(progress.Min(), runner.lastResolved) >= 0 {
			break
		}
		log.Infof("waiting for resolved timestamp progress: %s vs %s", progress, runner.lastResolved)
		select {
		case <-changed:
		case <-ctx.Done():
			r.NoError(ctx.Err())
		}
	}
	log.Infof("resolved timestamps have caught up; validating workload")

	workload.CheckConsistent(ctx, t)
}

// This is a black-box test to ensure the demo command fires up.
func TestDemoCommand(t *testing.T) {
	const testTime = 5 * time.Second
	t.Parallel()
	r := require.New(t)

	fixture, err := base.NewFixture(t)
	r.NoError(err)

	ctx := fixture.Context
	go func() {
		select {
		case <-time.After(testTime):
		case <-ctx.Done():
		}
		ctx.Stop(0)
	}()

	cmd := pcDemo()
	r.NoError(cmd.ParseFlags([]string{
		"--failFast", "true",
		"--stagingConn", fixture.StagingPool.ConnectionString,
		"--stagingSchema", fixture.StagingDB.Schema().Raw(),
		"--targetConn", fixture.TargetPool.ConnectionString,
	}))
	r.NoError(cmd.ExecuteContext(ctx))
}

// This is a black-box test to ensure the run command fires up.
func TestRunCommand(t *testing.T) {
	const testTime = 5 * time.Second
	t.Parallel()
	r := require.New(t)

	fixture, err := all.NewFixture(t)
	r.NoError(err)

	ctx := fixture.Context
	serverCfg := &server.Config{
		HTTP: stdserver.Config{
			BindAddr:           "127.0.0.1:0",
			GenerateSelfSigned: true,
		},
		Staging: sinkprod.StagingConfig{
			CommonConfig: sinkprod.CommonConfig{
				Conn: fixture.StagingPool.ConnectionString,
			},
			CreateSchema: true,
			Schema:       fixture.StagingDB.Schema(),
		},
		Target: sinkprod.TargetConfig{
			CommonConfig: sinkprod.CommonConfig{
				Conn: fixture.TargetPool.ConnectionString,
			},
		},
	}

	cfg := &clientConfig{}
	svr, err := cfg.newServer(ctx, serverCfg)
	r.NoError(err)
	r.NoError(cfg.createTables(ctx, svr.TargetPool))
	r.NoError(cfg.initURL(svr.GetListener()))
	r.NoError(cfg.generateJWT(ctx, svr))

	go func() {
		select {
		case <-time.After(testTime):
		case <-ctx.Done():
		}
		ctx.Stop(0)
	}()

	cmd := pcRun()
	r.NoError(cmd.ParseFlags([]string{
		"--failFast", "true",
		"--token", cfg.token,
		"--url", cfg.url,
	}))
	r.NoError(cmd.ExecuteContext(ctx))
}
