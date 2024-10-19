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

package jwt

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/replicator/internal/sinktest/all"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/stretchr/testify/assert"
)

// Generate a new token and validate it.
func TestJWT(t *testing.T) {
	a := assert.New(t)

	fixture, err := all.NewFixture(t, time.Minute)
	if !a.NoError(err) {
		return
	}

	ctx := fixture.Context
	pool := fixture.StagingPool
	stagingDB := fixture.StagingDB

	auth, err := ProvideAuth(ctx, pool, stagingDB)
	if !a.NoError(err) {
		return
	}

	// Verify internal state.
	impl := auth.(*authenticator)
	a.Empty(impl.mu.publicKeys)

	method, priv, err := InsertTestingKey(ctx, pool, auth, stagingDB)
	if !a.NoError(err) {
		return
	}

	a.Len(impl.mu.publicKeys, 1)

	target := ident.MustSchema(ident.New("database"), ident.New("target"))
	inserted, tkn, err := Sign(method, priv, []ident.Schema{target})
	if !a.NoError(err) {
		return
	}
	a.NotEmpty(tkn)
	t.Log(tkn)

	ok, err := auth.Check(ctx, target, tkn)
	a.NoError(err)
	a.True(ok)

	// Revoke the token id and revalidate.
	a.NoError(InsertRevokedToken(ctx, pool, auth, stagingDB, inserted.ID))
	ok, err = auth.Check(ctx, target, tkn)
	a.NoError(err)
	a.False(ok)
}

func TestMatches(t *testing.T) {
	dbA := ident.New("databaseA")
	schA1 := ident.MustSchema(dbA, ident.New("schema1"))
	schA2 := ident.MustSchema(dbA, ident.New("schema2"))

	dbB := ident.New("databaseB")
	schB1 := ident.MustSchema(dbB, ident.New("schema1"))
	schB2 := ident.MustSchema(dbB, ident.New("schema2"))

	wildA := ident.MustSchema(dbA, wildcard)
	wild1 := ident.MustSchema(wildcard, ident.New("schema1"))
	world := ident.MustSchema(wildcard, wildcard)

	tcs := []struct {
		allowed, requested ident.Schema
		expect             bool
	}{
		{allowed: schA1, requested: schA1, expect: true},
		{allowed: schA1, requested: schA2, expect: false},
		{allowed: schA1, requested: schB1, expect: false},
		{allowed: schA1, requested: schB2, expect: false},

		{allowed: wildA, requested: schA1, expect: true},
		{allowed: wildA, requested: schA2, expect: true},
		{allowed: wildA, requested: schB1, expect: false},
		{allowed: wildA, requested: schB2, expect: false},

		{allowed: wild1, requested: schA1, expect: true},
		{allowed: wild1, requested: schA2, expect: false},
		{allowed: wild1, requested: schB1, expect: true},
		{allowed: wild1, requested: schB2, expect: false},

		{allowed: world, requested: schA1, expect: true},
		{allowed: world, requested: schA2, expect: true},
		{allowed: world, requested: schB1, expect: true},
		{allowed: world, requested: schB2, expect: true},
	}

	for idx, tc := range tcs {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			a := assert.New(t)
			a.Equalf(tc.expect, matches(tc.allowed, tc.requested),
				"allowed=%s requested=%s", tc.allowed, tc.requested)
		})
	}
}
