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
	"fmt"
	"testing"

	"github.com/cockroachdb/replicator/internal/sinktest/base"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/notify"
	"github.com/stretchr/testify/require"
)

const oldSchema = `
CREATE TABLE %[1]s (
  target_schema     STRING      NOT NULL,
  source_nanos      INT         NOT NULL,
  source_logical    INT         NOT NULL,
  first_seen        TIMESTAMPTZ NOT NULL DEFAULT now(),
  target_applied_at TIMESTAMPTZ,
  PRIMARY KEY (target_schema, source_nanos, source_logical)
)
`

// This ensures that we can transition from the old resolved_timestamps
// table schema to the new partitioned checkpoints schema.
func TestMigration(t *testing.T) {
	r := require.New(t)

	fixture, err := base.NewFixture(t)
	r.NoError(err)

	ctx := fixture.Context

	oldTable := ident.NewTable(fixture.StagingDB.Schema(),
		ident.New("resolved_timestamps"))
	_, err = fixture.StagingPool.Exec(ctx, fmt.Sprintf(oldSchema, oldTable))
	r.NoError(err)

	_, err = fixture.StagingPool.Exec(ctx, fmt.Sprintf(`
INSERT INTO %s (target_schema, source_nanos, source_logical, target_applied_at) VALUES 
 ('foo.public', 100, 1, '2024-04-22 15:01:06+00'),
 ('foo.public', 200, 2, '2024-04-22 15:02:06+00'),
 ('foo.public', 300, 3, NULL),
 ('foo.public', 400, 4, NULL),
 ('bar.baz', 700, 7, '2024-04-22 15:03:06+00'),
 ('bar.baz', 800, 8, NULL),
 ('quux.public', 900, 9, '2024-04-22 15:04:06+00')
`, oldTable))
	r.NoError(err)

	chk, err := ProvideCheckpoints(ctx, fixture.StagingPool, fixture.StagingDB)
	r.NoError(err)

	schemas, err := chk.ScanForTargetSchemas(ctx)
	r.NoError(err)
	r.Len(schemas, 2) // Only returns unresolved schemas.

	expected := map[string]hlc.Range{
		"foo.public":  hlc.RangeIncluding(hlc.New(200, 2), hlc.New(400, 4)),
		"bar.baz":     hlc.RangeIncluding(hlc.New(700, 7), hlc.New(800, 8)),
		"quux.public": hlc.RangeIncluding(hlc.New(900, 9), hlc.New(900, 9)),
	}
	for id, expect := range expected {
		rng, err := chk.newGroup(&types.TableGroup{
			Name: ident.New(id),
		}, notify.VarOf(hlc.RangeEmpty()),
		).refreshQuery(ctx, hlc.Zero())
		r.NoError(err)
		r.Equal(expect, rng)
	}

	// Verify writes are blocked.
	_, err = fixture.StagingPool.Exec(ctx, fmt.Sprintf(`
INSERT INTO %s (target_schema, source_nanos, source_logical, target_applied_at) VALUES 
 ('will.fail', 900, 9, '2024-04-22 15:04:06+00')
`, oldTable))
	code, ok := fixture.StagingPool.ErrCode(err)
	r.True(ok)
	r.Equal("23514", code) // check_violation

	// Ensure that the next instance to be created will succeed.
	_, err = ProvideCheckpoints(ctx, fixture.StagingPool, fixture.StagingDB)
	r.NoError(err)
}
