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

package votr

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/sinktest/base"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVOTR(t *testing.T) {
	if testing.Short() {
		t.Skip("long test")
		return
	}
	t.Run("2DC", func(t *testing.T) {
		testVOTR(t, 2)
	})
	t.Run("hub_spoke", func(t *testing.T) {
		testVOTR(t, 4)
	})
}

func testVOTR(t *testing.T, numDBs int) {
	a := assert.New(t)
	r := require.New(t)

	fixture, err := base.NewFixture(t)
	r.NoError(err)
	ctx := fixture.Context

	checkSupported(t, fixture.SourcePool)

	// Use all default values, except for the connect strings.
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	cfg := &config{}
	cfg.Bind(flags)
	r.NoError(flags.Parse(nil))
	cfg.Connect = make([]string, numDBs)
	for i := range cfg.Connect {
		cfg.Connect[i] = fixture.SourcePool.ConnectionString
	}
	cfg.DrainDelay = 30 * time.Second
	cfg.Enclosing, _ = fixture.TargetSchema.Split()
	cfg.ErrInconsistent = true
	cfg.MaxBallots = 2048

	// Initialize the schema.
	initCtx := stopper.WithContext(ctx)
	r.NoError(doInit(initCtx, cfg))
	initCtx.Stop(100 * time.Millisecond)
	r.NoError(initCtx.Wait())

	// doRun will exit on its own due to cfg.StopAfter. This function
	// will return an error if any inconsistencies were seen.
	runCtx := stopper.WithContext(ctx)
	r.NoError(doRun(runCtx, cfg))
	r.NoError(runCtx.Wait())

	// Final validation of totals. We know that ballots and totals are
	// consistent within a destination schema, but we want to check that
	// the totals are consistent across the various SQL databases.
	for idx, s := range cfg.schemas {
		var total int
		r.NoError(fixture.SourcePool.QueryRowContext(ctx,
			fmt.Sprintf("SELECT sum(total) FROM %s", s.totals),
		).Scan(&total))
		a.Equalf(cfg.MaxBallots, total, "idx %d", idx)
	}
}
