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

// Package sinkprod contains configuration and providers for connecting
// to production database(s).
package sinkprod

import (
	"time"

	"github.com/google/wire"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideStagingDB,
	ProvideStagingPool,
	ProvideTargetPool,
	ProvideStatementCache,
)

const (
	defaultApplyTimeout = 30 * time.Second
	defaultLifetime     = 10 * time.Minute
	defaultCacheSize    = 128
	defaultPoolSize     = 128
)
