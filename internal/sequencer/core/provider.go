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

package core

import (
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/sequencer/scheduler"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/google/wire"
)

// Set is used by Wire.
var Set = wire.NewSet(ProvideCore)

// ProvideCore is called by Wire.
func ProvideCore(
	cfg *sequencer.Config,
	leases types.Leases,
	scheduler *scheduler.Scheduler,
	targetPool *types.TargetPool,
) *Core {
	return &Core{
		cfg:        cfg,
		leases:     leases,
		scheduler:  scheduler,
		targetPool: targetPool,
	}
}
