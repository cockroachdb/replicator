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

package votr

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/sinktest/base"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/stretchr/testify/require"
)

func TestSchema(t *testing.T) {
	const numCandidates = 128
	r := require.New(t)

	fixture, err := base.NewFixture(t)
	r.NoError(err)

	checkSupported(t, fixture.SourcePool)

	ctx := fixture.Context

	// Steal the enclosing DATABASE name, since we're basically running
	// the votr init command.
	enclosingDB := fixture.SourceSchema.Schema().Idents(nil)[0]
	sch := newSchema(fixture.SourcePool, enclosingDB, 0)

	// Set up the schema, insert some votes, and ensure that everything
	// is consistent.
	r.NoError(sch.create(ctx))
	r.NoError(sch.ensureCandidates(ctx, numCandidates))
	for i := 0; i < 10; i++ {
		r.NoError(sch.doStuff(ctx, 10))
	}
	failures, err := sch.validate(ctx, false)
	r.NoError(err)
	r.Empty(failures)

	// Break the totals table.
	_, err = fixture.SourcePool.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET total=total+1 WHERE true`, sch.totals))
	r.NoError(err)
	failures, err = sch.validate(ctx, false)
	r.NoError(err)
	r.NotEmpty(failures)
}

// checkSupported calls t.Skip() if VOTR cannot run on the target
// CockroachDB version.
func checkSupported(t *testing.T, target *types.SourcePool) {
	if target.Product != types.ProductCockroachDB ||
		strings.Contains(target.Version, "v21.") ||
		strings.Contains(target.Version, "v22.") {
		t.Skipf("VOTR only runs on CRDB 23.1 or later")
	}
}
