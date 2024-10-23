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

package schemawatch

import (
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/diag"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/google/wire"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideBackup,
	ProvideFactory,
)

// ProvideFactory is called by Wire to construct the Watchers factory.
func ProvideFactory(
	ctx *stopper.Context, cfg *Config, pool *types.TargetPool, d *diag.Diagnostics, b Backup,
) (types.Watchers, error) {
	w := &factory{cfg: cfg, pool: pool, stop: ctx, backup: b}
	w.mu.data = &ident.SchemaMap[*watcher]{}
	if err := d.Register("schema", w); err != nil {
		return nil, err
	}
	return w, nil
}

// ProvideBackup provides the Backup service used by Watcher
func ProvideBackup(m types.Memo, stagingPool *types.StagingPool) Backup {
	return &memoBackup{
		memo:        m,
		stagingPool: stagingPool,
	}
}
