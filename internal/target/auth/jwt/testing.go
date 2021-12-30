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
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"fmt"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/golang-jwt/jwt/v4"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/pkg/errors"
)

// InsertTestingKey generates a new private key and updates the existing
// Authenticator with the associated public key.
func InsertTestingKey(
	ctx context.Context, tx pgxtype.Querier, auth types.Authenticator, stagingDB ident.Ident,
) (method jwt.SigningMethod, signer crypto.PrivateKey, err error) {
	impl, ok := auth.(*authenticator)
	if !ok {
		err = errors.Errorf("unexpected Authenticator type %t", auth)
		return
	}

	method, signer, bytes, err := makeKeyBytes()
	if err != nil {
		return
	}

	err = insertKey(ctx, tx, stagingDB, bytes)
	if err != nil {
		return
	}

	err = impl.refresh(ctx, tx)
	return
}

// insertKey inserts a PEM-encoded key into the database.
func insertKey(
	ctx context.Context, tx pgxtype.Querier, stagingDB ident.Ident, pemBytes []byte,
) error {
	keyTable := ident.NewTable(stagingDB, ident.Public, PublicKeysTable)
	_, err := tx.Exec(ctx,
		fmt.Sprintf("INSERT INTO %s (public_key) VALUES ($1)", keyTable),
		pemBytes,
	)
	return errors.WithStack(err)
}

// makeKeyBytes creates a private key and its PEM-encoded representation.
func makeKeyBytes() (jwt.SigningMethod, crypto.PrivateKey, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, nil, errors.WithStack(err)
	}

	keyBytes, err := x509.MarshalPKIXPublicKey(key.Public())
	if err != nil {
		return nil, nil, nil, errors.WithStack(err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: keyBytes})
	return jwt.SigningMethodES256, key, pemBytes, nil
}

// InsertRevokedToken inserts the id of a revoked token into the
// Authenticator.
func InsertRevokedToken(
	ctx context.Context,
	tx pgxtype.Querier,
	auth types.Authenticator,
	stagingDB ident.Ident,
	id string,
) error {
	impl, ok := auth.(*authenticator)
	if !ok {
		return errors.Errorf("unexpected Authenticator type %t", auth)
	}

	keyTable := ident.NewTable(stagingDB, ident.Public, RevokedIdsTable)
	_, err := tx.Exec(ctx,
		fmt.Sprintf("INSERT INTO %s (id) VALUES ($1)", keyTable),
		id,
	)
	if err != nil {
		return errors.WithStack(err)
	}

	return impl.refresh(ctx, tx)
}
