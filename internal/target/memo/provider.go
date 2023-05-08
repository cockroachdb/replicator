// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package memo

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/google/wire"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideMemo,
	wire.Bind(new(types.Memo), new(*Memo)),
)

// ProvideMemo is called by Wire to construct the KV wrapper.
func ProvideMemo(ctx context.Context, db *pgxpool.Pool, staging ident.StagingDB) (*Memo, error) {
	target := ident.NewTable(staging.Ident(), ident.Public, ident.New("memo"))
	if err := retry.Execute(ctx, db, fmt.Sprintf(schema, target)); err != nil {
		return nil, err
	}
	ret := &Memo{}
	ret.sql.get = fmt.Sprintf(getTemplate, target)
	ret.sql.update = fmt.Sprintf(updateTemplate, target)
	return ret, nil
}
