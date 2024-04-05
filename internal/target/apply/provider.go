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

package apply

import (
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/applycfg"
	"github.com/cockroachdb/cdc-sink/internal/util/diag"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/google/wire"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideAcceptor,
)

// ProvideAcceptor is called by Wire.
func ProvideAcceptor(
	ctx *stopper.Context,
	cache *types.TargetStatements,
	configs *applycfg.Configs,
	diags *diag.Diagnostics,
	dlqs types.DLQs,
	target *types.TargetPool,
	watchers types.Watchers,
) (*Acceptor, error) {
	f := &factory{
		cache:    cache,
		configs:  configs,
		dlqs:     dlqs,
		poolInfo: target.Info(),
		stop:     ctx,
		watchers: watchers,
	}
	f.mu.instances = &ident.TableMap[*apply]{}
	if err := diags.Register("apply", f); err != nil {
		return nil, err
	}

	a := &Acceptor{
		factory:    f,
		targetPool: target,
	}
	return a, nil
}
