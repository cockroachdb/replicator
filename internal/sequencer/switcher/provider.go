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

package switcher

import (
	"github.com/cockroachdb/replicator/internal/sequencer/besteffort"
	"github.com/cockroachdb/replicator/internal/sequencer/chaos"
	"github.com/cockroachdb/replicator/internal/sequencer/core"
	"github.com/cockroachdb/replicator/internal/sequencer/decorators"
	"github.com/cockroachdb/replicator/internal/sequencer/immediate"
	"github.com/cockroachdb/replicator/internal/sequencer/scheduler"
	"github.com/cockroachdb/replicator/internal/sequencer/script"
	"github.com/cockroachdb/replicator/internal/sequencer/staging"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/diag"
	"github.com/google/wire"
)

// Set is used by Wire.
var Set = wire.NewSet(
	besteffort.Set,
	chaos.Set,
	core.Set,
	immediate.Set,
	decorators.Set,
	script.Set,
	scheduler.Set,
	staging.Set,

	ProvideSequencer,
)

// ProvideSequencer is called by Wire.
func ProvideSequencer(
	best *besteffort.BestEffort,
	core *core.Core,
	diags *diag.Diagnostics,
	imm *immediate.Immediate,
	stg *staging.Staging,
	stagingPool *types.StagingPool,
	targetPool *types.TargetPool,
) *Switcher {
	return &Switcher{
		bestEffort:  best,
		core:        core,
		diags:       diags,
		immediate:   imm,
		staging:     stg,
		stagingPool: stagingPool,
		targetPool:  targetPool,
	}
}
