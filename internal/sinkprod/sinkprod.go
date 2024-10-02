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

	"github.com/cockroachdb/replicator/internal/sinktest"
	"github.com/cockroachdb/replicator/internal/util/stdpool"
	"github.com/google/wire"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideStagingDB,
	ProvideStagingPool,
	ProvideTargetPool,
	stdpool.ProvideBackup,
	ProvideStatementCache,
	sinktest.NewBreakers,
)

const (
	defaultApplyTimeout = 30 * time.Second
	defaultCacheSize    = 128
	defaultJitterTime   = 15 * time.Second
	defaultIdleTime     = 1 * time.Minute
	defaultMaxLifetime  = 5 * time.Minute
	defaultMaxPoolSize  = 128
)
