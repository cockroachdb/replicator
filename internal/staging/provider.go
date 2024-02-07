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

// Package staging contains all services which interact with the staging
// database.
package staging

import (
	"github.com/cockroachdb/cdc-sink/internal/staging/leases"
	"github.com/cockroachdb/cdc-sink/internal/staging/memo"
	"github.com/cockroachdb/cdc-sink/internal/staging/resolved"
	"github.com/cockroachdb/cdc-sink/internal/staging/stage"
	"github.com/cockroachdb/cdc-sink/internal/staging/version"
	"github.com/cockroachdb/cdc-sink/internal/util/applycfg"
	"github.com/google/wire"
)

// Set is used by Wire and contains providers for all target
// sub-packages.
var Set = wire.NewSet(
	applycfg.Set,
	leases.Set,
	memo.Set,
	stage.Set,
	resolved.Set,
	version.Set,
)
