// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package timekeeper

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/google/wire"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/pkg/errors"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideTimeKeeper,
)

// TargetTable is a configuration point for TimeKeeper.
type TargetTable ident.Table

// Table returns the target table.
func (t TargetTable) Table() ident.Table { return ident.Table(t) }

// ProvideTimeKeeper is called by Wire.
func ProvideTimeKeeper(
	ctx context.Context, tx pgxtype.Querier, target TargetTable,
) (types.TimeKeeper, func(), error) {
	if err := retry.Execute(ctx, tx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
key STRING NOT NULL,
nanos INT8 NOT NULL,
logical INT8 NOT NULL,
PRIMARY KEY (key, nanos, logical)
)
`, target.Table())); err != nil {
		return nil, func() {}, errors.WithStack(err)
	}

	ret := &timekeeper{pool: tx}
	ret.mu.cleanups = make(map[ident.Schema]hlc.Time)
	ret.sql.append = fmt.Sprintf(appendTemplate, target.Table())
	ret.sql.cleanup = fmt.Sprintf(cleanupTemplate, target.Table())

	// Start a background process to purge old resolved timestamps.
	ctx, cancel := context.WithCancel(ctx)
	if *cleanupDelay > 0 {
		go ret.cleanupLoop(ctx)
	}

	return ret, cancel, nil
}
