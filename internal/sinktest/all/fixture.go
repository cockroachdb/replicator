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

package all

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cdc-sink/internal/sinktest/base"
	"github.com/cockroachdb/cdc-sink/internal/staging/version"
	"github.com/cockroachdb/cdc-sink/internal/target/dlq"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/applycfg"
	"github.com/cockroachdb/cdc-sink/internal/util/diag"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
)

// Fixture provides a complete set of database-backed services. One can
// be constructed by calling NewFixture or by incorporating TestSet into
// a Wire provider set.
type Fixture struct {
	*base.Fixture

	Appliers       types.Appliers
	Configs        *applycfg.Configs
	Diagnostics    *diag.Diagnostics
	DLQConfig      *dlq.Config
	DLQs           types.DLQs
	Memo           types.Memo
	Stagers        types.Stagers
	VersionChecker *version.Checker
	Watchers       types.Watchers

	Watcher types.Watcher // A watcher for TestDB.
}

// CreateDLQTable ensures that a DLQ table exists. The name of the table
// is returned so that tests may inspect it.
func (f *Fixture) CreateDLQTable(ctx context.Context) (ident.Table, error) {
	create := dlq.BasicSchemas[f.TargetPool.Product]
	dlqTable := ident.NewTable(f.TargetSchema.Schema(), f.DLQConfig.TableName)
	if _, err := f.TargetPool.ExecContext(ctx, fmt.Sprintf(create, dlqTable)); err != nil {
		return ident.Table{}, errors.WithStack(err)
	}
	if err := f.Watcher.Refresh(ctx, f.TargetPool); err != nil {
		return ident.Table{}, err
	}
	return dlqTable, nil
}

// CreateTargetTable creates a test table within the TargetPool and
// TargetSchema. If the table is successfully created, the schema
// watcher will be refreshed. The schemaSpec parameter must have exactly
// one %s substitution parameter for the database name and table name.
func (f *Fixture) CreateTargetTable(
	ctx context.Context, schemaSpec string,
) (base.TableInfo[*types.TargetPool], error) {
	ti, err := f.Fixture.CreateTargetTable(ctx, schemaSpec)
	if err == nil {
		err = f.Watcher.Refresh(ctx, f.TargetPool)
	}
	return ti, err
}

// PeekStaged peeks at the data which has been staged for the target
// table between the given timestamps.
func (f *Fixture) PeekStaged(
	ctx context.Context, tbl ident.Table, startAt, endBefore hlc.Time,
) ([]types.Mutation, error) {
	tx, err := f.StagingPool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	q := &types.UnstageCursor{
		StartAt:   startAt,
		EndBefore: endBefore,
		Targets:   []ident.Table{tbl},
	}
	var ret []types.Mutation
	for selecting := true; selecting; {
		q, selecting, err = f.Stagers.Unstage(ctx, tx, q,
			func(ctx context.Context, tbl ident.Table, mut types.Mutation) error {
				ret = append(ret, mut)
				return nil
			})
		if err != nil {
			return nil, err
		}
	}
	return ret, nil
}
