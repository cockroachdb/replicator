// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package all

import (
	"context"

	"github.com/cockroachdb/cdc-sink/internal/sinktest/base"
	"github.com/cockroachdb/cdc-sink/internal/target/apply"
	"github.com/cockroachdb/cdc-sink/internal/types"
)

// Fixture provides a complete set of database-backed services. One can
// be constructed by calling NewFixture or by incorporating TestSet into
// a Wire provider set.
type Fixture struct {
	*base.Fixture

	Appliers types.Appliers
	Configs  *apply.Configs
	Memo     types.Memo
	Stagers  types.Stagers
	Watchers types.Watchers

	Watcher types.Watcher // A watcher for TestDB.
}

// CreateTable creates a test table within the TestDB and refreshes the
// target database's Watcher. The schemaSpec parameter must have exactly
// one %s substitution parameter for the database name and table name.
func (f *Fixture) CreateTable(ctx context.Context, schemaSpec string) (base.TableInfo, error) {
	ti, err := f.Fixture.CreateTable(ctx, schemaSpec)
	if err == nil {
		err = f.Watcher.Refresh(ctx, f.Pool)
	}
	return ti, err
}
