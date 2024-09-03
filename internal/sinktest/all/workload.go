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

package all

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/replicator/internal/sinktest/base"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/workload"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

// Workload configures a [workload.Checker] to interact with tables in
// the staging and target databases.
type Workload struct {
	*workload.Checker
	Parent, Child base.TableInfo[*types.TargetPool]

	fixture *Fixture
}

// WorkloadConfig provides additional parameters to
// [Fixture.NewWorkload].
type WorkloadConfig struct {
	DisableFK      bool // Don't create FK references from child to parent.
	DisableStaging bool // Don't run any tests that involve the staging tables.
}

// NewWorkload constructs a parent/child workload test rig attached to
// the test fixture.
func (f *Fixture) NewWorkload(
	ctx context.Context, cfg *WorkloadConfig,
) (*Workload, *types.TableGroup, error) {
	// We want at least a 64-bit value.
	bigType := "BIGINT"
	if f.TargetPool.Product == types.ProductOracle {
		bigType = "NUMBER(38)"
	}

	parentInfo, err := f.CreateTargetTable(ctx, fmt.Sprintf(
		"CREATE TABLE %%s (parent %[1]s PRIMARY KEY, val %[1]s DEFAULT 0 NOT NULL)",
		bigType))
	if err != nil {
		return nil, nil, err
	}

	// The child table may be generated with or without an FK reference.
	childSchema := fmt.Sprintf(
		`CREATE TABLE %%s (
child %[2]s PRIMARY KEY,
parent %[2]s NOT NULL,
val %[2]s DEFAULT 0 NOT NULL,
CONSTRAINT parent_fk FOREIGN KEY(parent) REFERENCES %[1]s(parent)
)`, parentInfo.Name(), bigType)
	if cfg.DisableFK {
		childSchema = fmt.Sprintf(
			`CREATE TABLE %%s (
child %[1]s PRIMARY KEY,
parent %[1]s NOT NULL,
val %[1]s DEFAULT 0 NOT NULL
)`, bigType)
	}

	childInfo, err := f.CreateTargetTable(ctx, childSchema)
	if err != nil {
		return nil, nil, err
	}

	return &Workload{
			Checker: &workload.Checker{
				GeneratorBase: workload.NewGeneratorBase(parentInfo.Name(), childInfo.Name()),
				LoadChild: func(id int) (parent int, val int64, ok bool, err error) {
					if err = f.TargetPool.QueryRowContext(ctx,
						fmt.Sprintf("SELECT parent, val FROM %s WHERE child=%d",
							childInfo.Name(), id),
					).Scan(&parent, &val); err == nil {
						ok = true
					} else if errors.Is(err, sql.ErrNoRows) {
						err = nil
					}
					return
				},
				LoadParent: func(id int) (val int64, ok bool, err error) {
					if err = f.TargetPool.QueryRowContext(ctx,
						fmt.Sprintf("SELECT val FROM %s WHERE parent=%d",
							parentInfo.Name(), id),
					).Scan(&val); err == nil {
						ok = true
					} else if errors.Is(err, sql.ErrNoRows) {
						err = nil
					}
					return
				},
				StageCounter: func(table ident.Table, rng hlc.Range) (int, error) {
					found, err := f.PeekStaged(ctx, table, rng)
					return len(found), err
				},
				RowCounter: func(table ident.Table) (int, error) {
					switch table.Canonical().Raw() {
					case parentInfo.Name().Canonical().Raw():
						return parentInfo.RowCount(ctx)
					case childInfo.Name().Canonical().Raw():
						return childInfo.RowCount(ctx)
					default:
						return 0, errors.Errorf("unknown table %s", table)
					}
				},
			},
			Child:   childInfo,
			fixture: f,
			Parent:  parentInfo,
		},
		&types.TableGroup{
			Name:      ident.New(f.TargetSchema.Schema().Raw()),
			Enclosing: f.TargetSchema.Schema(),
			Tables:    []ident.Table{childInfo.Name(), parentInfo.Name()},
		},
		nil
}

// CheckConsistent verifies that the staging tables are empty and that
// the requisite number of rows exist in the target tables.
func (g *Workload) CheckConsistent(_ context.Context, t testing.TB) (ok bool) {
	a := assert.New(t)
	failures, err := g.Checker.CheckConsistent()
	if a.NoError(err) {
		return a.Empty(failures)
	}
	return false
}
