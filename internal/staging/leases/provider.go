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

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/google/wire"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideLeases,
)

// ProvideLeases is called by Wire to configure the work-leasing strategy.
//
// This can be removed once stagingDB once SELECT FOR UPDATE uses
// replicated locks.
//
// https://github.com/cockroachdb/cockroach/issues/100194
func ProvideLeases(
	ctx context.Context, pool types.StagingPool, stagingDB ident.StagingDB,
) (types.Leases, error) {
	return New(ctx, Config{
		Guard:      time.Second,
		Lifetime:   5 * time.Second,
		RetryDelay: time.Second,
		Poll:       time.Second,
		Pool:       pool,
		Target:     ident.NewTable(stagingDB.Ident(), ident.Public, ident.New("leases")),
	})
}
