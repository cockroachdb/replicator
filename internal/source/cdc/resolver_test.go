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

package cdc

import (
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/sinktest/all"
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolverDeQueue(t *testing.T) {
	const rowCount = 100
	a := assert.New(t)
	r := require.New(t)

	baseFixture, cancel, err := all.NewFixture()
	r.NoError(err)
	defer cancel()

	fixture, cancel, err := newTestFixture(baseFixture, &Config{
		MetaTableName: ident.New("resolved_timestamps"),
		BaseConfig: logical.BaseConfig{
			StagingSchema: baseFixture.StagingDB.Schema(),
			StagingConn:   baseFixture.StagingPool.ConnectionString,
			TargetConn:    baseFixture.TargetPool.ConnectionString,
		},
	})
	r.NoError(err)
	defer cancel()

	ctx := fixture.Context
	tbl, err := fixture.CreateTargetTable(ctx,
		`CREATE TABLE %s (pk INT PRIMARY KEY, v INT NOT NULL)`)
	r.NoError(err)

	// Disable call to loop.Start().
	fixture.Resolvers.noStart = true
	resolver, err := fixture.Resolvers.get(ctx, tbl.Name().Schema())
	r.NoError(err)

	for i := int64(0); i < rowCount; i++ {
		r.NoError(resolver.Mark(ctx, hlc.New(i+1, 0)))
		r.NoError(resolver.Mark(ctx, hlc.New(i, 0)))
	}

	log.Info("marked")

	var committed hlc.Time
	for i := 0; i < rowCount; i++ {
		found, err := resolver.selectTimestamp(ctx, committed)
		r.NoError(err)
		a.Equal(int64(i), committed.Nanos()) // Verify expected order.

		r.NoError(resolver.Record(ctx, found))
		committed = found
	}
	// Make sure we arrived at the end.
	a.Equal(hlc.New(rowCount, 0), committed)

	// Verify empty queue.  We may need to wait for a previous
	// transaction to commit.
	_, err = resolver.selectTimestamp(ctx, committed)
	a.Equal(errNoWork, err)
}
