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

package applycfg

import (
	"github.com/cockroachdb/replicator/internal/util/diag"
	"github.com/google/wire"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideConfigs,
)

// ProvideConfigs is called by Wire to construct a Configs instance.
func ProvideConfigs(diags *diag.Diagnostics) (*Configs, error) {
	cfg := &Configs{}
	if err := diags.Register("applycfg", cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
