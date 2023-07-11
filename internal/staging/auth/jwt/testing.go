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
	"github.com/pkg/errors"
)

// InsertTestingKey generates a new private key and updates the existing
// Authenticator with the associated public key.
func InsertTestingKey(
	ctx context.Context, tx types.StagingQuerier, auth types.Authenticator, stagingDB ident.StagingDB,
) (method jwt.SigningMethod, signer crypto.PrivateKey, err error) {
	impl, ok := auth.(*authenticator)
	if !ok {
		err = errors.Errorf("unexpected Authenticator type %t", auth)
		return
	}

	var bytes []byte
	method, signer, bytes, err = makeKeyBytes()
	if err != nil {
		return
	}

	err = insertKey(ctx, tx, stagingDB.Schema(), bytes)
	if err != nil {
		return
	}

	err = impl.refresh(ctx, tx)
	return
}

// insertKey inserts a PEM-encoded key into the database.
func insertKey(
	ctx context.Context, tx types.StagingQuerier, stagingDB ident.Schema, pemBytes []byte,
) error {
	keyTable := ident.NewTable(stagingDB, PublicKeysTable)
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
	tx types.StagingQuerier,
	auth types.Authenticator,
	stagingDB ident.StagingDB,
	id string,
) error {
	impl, ok := auth.(*authenticator)
	if !ok {
		return errors.Errorf("unexpected Authenticator type %t", auth)
	}

	keyTable := ident.NewTable(stagingDB.Schema(), RevokedIdsTable)
	_, err := tx.Exec(ctx,
		fmt.Sprintf("INSERT INTO %s (id) VALUES ($1)", keyTable),
		id,
	)
	if err != nil {
		return errors.WithStack(err)
	}

	return impl.refresh(ctx, tx)
}
