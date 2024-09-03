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

package leases

import (
	"context"
	"time"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/google/wire"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideLeases,
)

// ProvideLeases is called by Wire to configure the work-leasing strategy.
func ProvideLeases(
	ctx context.Context, pool *types.StagingPool, stagingDB ident.StagingSchema,
) (types.Leases, error) {
	target := pool.HintNoFTS(ident.NewTable(stagingDB.Schema(), ident.New("leases")))
	return New(ctx, Config{
		Guard:      time.Second,
		Lifetime:   5 * time.Second,
		RetryDelay: time.Second,
		Poll:       time.Second,
		Pool:       pool,
		Target:     target,
	})
}
