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
	"crypto"

	"github.com/cockroachdb/replicator/internal/util/ident"
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
			Issuer: "replicator",
		},
		Ext: ClaimData{
			Schemas: schemas,
		},
	}
	return cl, nil
}

// Sign generates a new JWT that is compatible with Replicator. This
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
