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
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/target/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/stretchr/testify/assert"
)

// Generate a new token and validate it.
func TestJWT(t *testing.T) {
	a := assert.New(t)

	ctx, dbInfo, cancel := sinktest.Context()
	defer cancel()

	dbName, cancel, err := sinktest.CreateDB(ctx)
	defer cancel()
	if !a.NoError(err) {
		return
	}

	auth, cancel, err := New(ctx, dbInfo.Pool(), dbName)
	defer cancel()
	if !a.NoError(err) {
		return
	}

	method, priv, err := InsertTestingKey(ctx, dbInfo.Pool(), auth, dbName)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	// Verify internal state.
	impl := auth.(*authenticator)
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
	a.NoError(InsertRevokedToken(ctx, dbInfo.Pool(), auth, dbName, inserted.ID))
	ok, err = auth.Check(ctx, target, tkn)
	a.NoError(err)
	a.False(ok)

}
