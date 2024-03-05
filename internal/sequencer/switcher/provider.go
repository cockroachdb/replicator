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
	"github.com/cockroachdb/cdc-sink/internal/sequencer/besteffort"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/chaos"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/immediate"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/scheduler"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/script"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/serial"
	"github.com/cockroachdb/cdc-sink/internal/sequencer/shingle"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/diag"
	"github.com/google/wire"
)

// Set is used by Wire.
var Set = wire.NewSet(
	besteffort.Set,
	chaos.Set,
	immediate.Set,
	script.Set,
	serial.Set,
	scheduler.Set,
	shingle.Set,

	ProvideSequencer,
)

// ProvideSequencer is called by Wire.
func ProvideSequencer(
	best *besteffort.BestEffort,
	diags *diag.Diagnostics,
	imm *immediate.Immediate,
	script *script.Sequencer,
	serial *serial.Serial,
	shingle *shingle.Shingle,
	stagingPool *types.StagingPool,
	targetPool *types.TargetPool,
) *Switcher {
	return &Switcher{
		bestEffort:  best,
		diags:       diags,
		immediate:   imm,
		script:      script,
		serial:      serial,
		shingle:     shingle,
		stagingPool: stagingPool,
		targetPool:  targetPool,
	}
}
