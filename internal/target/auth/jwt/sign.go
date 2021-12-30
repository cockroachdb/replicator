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
	"crypto"

	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/gofrs/uuid"
	"github.com/golang-jwt/jwt/v4"
	"github.com/pkg/errors"
)

// NewClaim returns a minimal clam that would provide access to the
// requested schemas.
func NewClaim(schemas []ident.Schema) (Claims, error) {
	uuid, err := uuid.NewV4()
	if err != nil {
		return Claims{}, errors.WithStack(err)
	}

	cl := Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			ID:     uuid.String(),
			Issuer: "cdc-sink",
		},
		Ext: ClaimData{
			Schemas: schemas,
		},
	}
	return cl, nil
}

// Sign generates a new JWT that is compatible with cdc-sink. This
// method is used for testing and for the example quickstart.
func Sign(
	method jwt.SigningMethod, key crypto.PrivateKey, schemas []ident.Schema,
) (Claims, string, error) {
	cl, err := NewClaim(schemas)
	if err != nil {
		return cl, "", err
	}

	tkn := jwt.New(method)
	tkn.Claims = cl
	ret, err := tkn.SignedString(key)
	return cl, ret, errors.WithStack(err)
}
