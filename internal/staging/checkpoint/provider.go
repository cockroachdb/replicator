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

package checkpoint

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/google/wire"
	"github.com/pkg/errors"
)

// Set is used by Wire.
var Set = wire.NewSet(ProvideCheckpoints)

// ProvideCheckpoints is called by Wire.
func ProvideCheckpoints(
	ctx context.Context, pool *types.StagingPool, meta ident.StagingSchema,
) (*Checkpoints, error) {
	// We could change this table name by probing for old vs. new table.
	// It's an implementation detail and the interesting questions are
	// already answered by the prometheus metrics.
	metaTable := ident.NewTable(meta.Schema(), ident.New("resolved_timestamps"))
	ddl := schema
	if strings.Contains(pool.Version, "v21.") || strings.Contains(pool.Version, "v22.1.") {
		ddl = schemaNoTimestamp
	}
	if _, err := pool.Exec(ctx, fmt.Sprintf(ddl, metaTable)); err != nil {
		return nil, errors.WithStack(err)
	}

	return &Checkpoints{
		pool:      pool,
		metaTable: metaTable,
	}, nil
}
