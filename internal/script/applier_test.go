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

package script

import (
	"context"
	"testing"

	"github.com/cockroachdb/replicator/internal/sinktest/mutations"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/stretchr/testify/require"
)

func TestMutOpsConversions(t *testing.T) {
	r := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const mutCount = 1024
	now := hlc.New(100, 100)

	muts := make([]types.Mutation, mutCount)
	source := mutations.Generator(ctx, 1024, 0.1)
	for idx := range muts {
		muts[idx] = <-source
		muts[idx].Time = now
	}
	table := ident.NewTable(
		ident.MustSchema(ident.New("db"), ident.New("public")),
		ident.New("table"))
	batch := &types.TableBatch{
		Data:  muts,
		Table: table,
		Time:  now,
	}

	ops, err := batchToOps(ctx, batch)
	r.NoError(err)
	r.Len(ops, mutCount)

	next := batch.Empty()
	r.NoError(opsIntoBatch(ctx, ops, next))
	r.Len(next.Data, mutCount)
	for idx, mut := range muts {
		r.Truef(mutations.Equal(muts[idx], mut), "mutations differ: %s vs %s", muts[idx], mut)
	}
}
