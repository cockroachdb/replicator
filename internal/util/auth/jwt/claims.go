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
