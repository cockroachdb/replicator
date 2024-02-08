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

package besteffort

import (
	"github.com/cockroachdb/cdc-sink/internal/sequencer"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/google/wire"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideBestEffort,
)

// ProvideBestEffort is called by Wire.
func ProvideBestEffort(
	cfg *sequencer.Config,
	leases types.Leases,
	stagingPool *types.StagingPool,
	stagers types.Stagers,
	targetPool *types.TargetPool,
	watchers types.Watchers,
) *BestEffort {
	return &BestEffort{
		cfg:         cfg,
		leases:      leases,
		stagingPool: stagingPool,
		stagers:     stagers,
		targetPool:  targetPool,
		watchers:    watchers,
	}
}
