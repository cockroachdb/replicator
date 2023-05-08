// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stage

import (
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/google/wire"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideFactory,
)

// ProvideFactory is called by Wire to construct the Stagers factory.
func ProvideFactory(db *pgxpool.Pool, stagingDB ident.StagingDB) types.Stagers {
	f := &factory{
		db:        db,
		stagingDB: stagingDB.Ident(),
	}
	f.mu.instances = make(map[ident.Table]*stage)
	return f
}
