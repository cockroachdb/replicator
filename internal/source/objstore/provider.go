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

package objstore

import (
	"fmt"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/conveyor"
	"github.com/cockroachdb/replicator/internal/script"
	"github.com/cockroachdb/replicator/internal/source/objstore/bucket"
	"github.com/cockroachdb/replicator/internal/source/objstore/eventproc"
	"github.com/cockroachdb/replicator/internal/source/objstore/providers/local"
	"github.com/cockroachdb/replicator/internal/source/objstore/providers/s3"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/cdcjson"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/google/wire"
	"github.com/pkg/errors"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideConn,
	ProvideEagerConfig,
	ProvideConveyorConfig,
)

// ProvideEagerConfig is a hack to move up the evaluation of the user
// script so that the options callbacks can set any non-script-related
// CLI flags.
func ProvideEagerConfig(cfg *Config, _ *script.Loader) *EagerConfig {
	return (*EagerConfig)(cfg)
}

// ProvideConveyorConfig is called by Wire.
func ProvideConveyorConfig(cfg *Config) *conveyor.Config {
	return &cfg.Conveyor
}

// ProvideConn is called by Wire to construct this package's
// logical.Dialect implementation. There's a fake dependency on
// the script loader so that flags can be evaluated first.
func ProvideConn(
	ctx *stopper.Context,
	config *Config,
	conv *conveyor.Conveyors,
	leases types.Leases,
	memo types.Memo,
	stagingPool *types.StagingPool,
	stagingSchema ident.StagingSchema,
) (*Conn, error) {
	if err := config.Preflight(ctx); err != nil {
		return nil, err
	}
	conveyors := conv.WithKind("objstore")
	if err := conveyors.Bootstrap(); err != nil {
		return nil, err
	}
	conveyor, err := conveyors.Get(config.TargetSchema)
	if err != nil {
		return nil, err
	}

	bucket, err := newBucket(config)
	if err != nil {
		return nil, err
	}
	parser, err := cdcjson.New(config.BufferSize)
	if err != nil {
		return nil, err
	}
	processor := eventproc.NewLocal(conveyor, bucket, parser, config.TargetSchema)

	conn := &Conn{
		bucket:      bucket,
		config:      config,
		conveyor:    conveyor,
		leases:      leases,
		parser:      parser,
		processor:   processor,
		stagingPool: stagingPool,
		state: state{
			memo: memo,
			key:  fmt.Sprintf("objstore_%s", config.bucketName),
		},
	}
	return (*Conn)(conn), conn.Start(ctx)
}

func newBucket(config *Config) (bucket.Bucket, error) {
	switch {
	case config.local != nil:
		return local.New(config.local)
	case config.s3 != nil:
		return s3.New(config.s3)
	default:
		return nil, errors.Errorf("invalid configuration. Missing bucket specification")
	}
}
