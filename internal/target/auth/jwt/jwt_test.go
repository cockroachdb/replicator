// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jwt

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/target/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/stretchr/testify/assert"
)

// Generate a new token and validate it.
func TestJWT(t *testing.T) {
	a := assert.New(t)

	fixture, cancel, err := sinktest.NewFixture()
	if !a.NoError(err) {
		return
	}
	defer cancel()

	ctx := fixture.Context
	pool := fixture.Pool
	stagingDB := fixture.StagingDB

	auth, cancel, err := ProvideAuth(ctx, pool, stagingDB)
	defer cancel()
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
	defer cancel()

	a.Len(impl.mu.publicKeys, 1)

	target := ident.NewSchema(ident.New("database"), ident.New("target"))
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
	schA1 := ident.NewSchema(dbA, ident.New("schema1"))
	schA2 := ident.NewSchema(dbA, ident.New("schema2"))

	dbB := ident.New("databaseB")
	schB1 := ident.NewSchema(dbB, ident.New("schema1"))
	schB2 := ident.NewSchema(dbB, ident.New("schema2"))

	wildA := ident.NewSchema(dbA, wildcard)
	wild1 := ident.NewSchema(wildcard, ident.New("schema1"))
	world := ident.NewSchema(wildcard, wildcard)

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
