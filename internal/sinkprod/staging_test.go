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

package sinkprod

import (
	"testing"

	"github.com/cockroachdb/replicator/internal/sinktest/base"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/stretchr/testify/require"
)

func TestLegacyStagingName(t *testing.T) {
	r := require.New(t)

	fixture, err := base.NewFixture(t)
	r.NoError(err)
	ctx := fixture.Context

	expect := func(cfg ident.Schema, expected ident.Schema) {
		found, err := ProvideStagingDB(ctx, &StagingConfig{
			Schema: cfg,
		}, fixture.StagingPool)
		r.NoError(err)
		r.Truef(ident.Equal(expected, found), "%s vs %s", expected, found)
	}
	foo := ident.MustSchema(ident.New("foo"), ident.Public)

	expect(ident.Schema{}, StagingSchemaDefault)
	expect(StagingSchemaDefault, StagingSchemaDefault)
	expect(StagingSchemaLegacy, StagingSchemaLegacy)
	expect(foo, foo)

	// We're going to swizzle the variable to an existing database to
	// avoid collision with a database instance used for local testing.
	old := StagingSchemaLegacy
	StagingSchemaLegacy = fixture.StagingDB.Schema()
	defer func() { StagingSchemaLegacy = old }()
	expect(ident.Schema{}, StagingSchemaLegacy)
	expect(StagingSchemaDefault, StagingSchemaLegacy)
	expect(StagingSchemaLegacy, StagingSchemaLegacy)
	expect(foo, foo)
}
