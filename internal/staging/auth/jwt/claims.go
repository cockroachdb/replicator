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
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/golang-jwt/jwt/v4"
)

// ClaimData is the custom data that we want to embed in a JWT token.
type ClaimData struct {
	Schemas []ident.Schema `json:"schemas,omitempty"`
}

// Claims extends the core JWT Claims with elements specific to
// cdc-sink.
type Claims struct {
	jwt.RegisteredClaims
	// Place our extension in a proper namespace, for compatibility with
	// e.g. Auth0.
	Ext ClaimData `json:"https://github.com/cockroachdb/cdc-sink,omitempty"`
}

var _ jwt.Claims = (*Claims)(nil)

// Valid implements jwt.Claims.
func (c Claims) Valid() error {
	// We use the id field for revocation, so require it.
	if c.ID == "" {
		return jwt.NewValidationError("id field is required", jwt.ValidationErrorId)
	}
	if len(c.Ext.Schemas) == 0 {
		return jwt.NewValidationError("no cdc-sink.schemas defined", jwt.ValidationErrorClaimsInvalid)
	}
	return c.RegisteredClaims.Valid()
}
