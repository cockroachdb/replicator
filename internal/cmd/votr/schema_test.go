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
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/sinktest/base"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/stretchr/testify/require"
)

func TestSchema(t *testing.T) {
	r := require.New(t)

	fixture, cancel, err := base.NewFixture()
	r.NoError(err)
	defer cancel()

	ctx := fixture.Context

	if fixture.TargetPool.Product != types.ProductCockroachDB {
		return
	}

	src := newSchema(fixture.SourcePool.DB, fixture.SourceSchema.Schema())

	r.NoError(src.create(ctx))

	r.NoError(src.ensureCandidates(ctx, 128))

	for i := 0; i < 10; i++ {
		r.NoError(src.doStuff(ctx, 10))
	}

	failures, err := src.validate(ctx, false)
	r.NoError(err)
	r.Empty(failures)
}
