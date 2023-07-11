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

// Package jwt contains support for using JWT tokens to authenticate and
// authorize incoming connections.
//
// We require that incoming tokens have
// been signed with either RSA or EC keys. The public keys used for
// validation are stored in the database and are periodically refreshed.
//
// Incoming JWT tokens are required to have the well-known "jti" token
// identifier field set. This is checked against a list of revoked token
// ids that are also stored in the database.
//
// A minimally-acceptable token is shown below:
//
//	{
//	   "jti": "a25dac04-9f3e-49c1-a068-ee0a2abbd7df",
//	   "https://github.com/cockroachdb/cdc-sink": {
//	     "sch": [
//	       [ "database", "schema" ],
//	       [ "another_database", "*" ],
//	       [ "*", "required_schema" ],
//	       [ "*", "*" ]
//	     ]
//	   }
//	}
//
// As seen above, we use a globally-unique namespace to provide a list
// of schemas that the caller is allowed to use. Additional fields may
// be added to this namespace in the future.
package jwt

import (
	"context"
	"crypto"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"sync"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/gofrs/uuid"
	"github.com/golang-jwt/jwt/v4"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

var (
	// PublicKeysTable is the name of the table that contains public
	// keys used to validate incoming JWT tokens.
	PublicKeysTable = ident.New("jwt_public_keys")
	// RefreshDelay controls how ofter a watcher will refresh its schema. If
	// this value is zero or negative, refresh behavior will be disabled.
	RefreshDelay = flag.Duration("jwtRefresh", time.Minute,
		"how often to scan for updated JWT configuration; set to zero to disable")
	// RevokedIdsTable allows a list of known-bad "jti" token ids to be
	// rejected.
	RevokedIdsTable = ident.New("jwt_revoked_ids")

	// Only permit RSA and ECC signatures (i.e. disallow the
	// perfectly-valid "none" algorithm). See also WithMethods() and
	// https://auth0.com/blog/critical-vulnerabilities-in-json-web-token-libraries/
	validJWTMethods = []string{
		jwt.SigningMethodES256.Alg(),
		jwt.SigningMethodES384.Alg(),
		jwt.SigningMethodES512.Alg(),
		jwt.SigningMethodRS256.Alg(),
		jwt.SigningMethodRS384.Alg(),
		jwt.SigningMethodRS512.Alg(),
	}
	// Used by matches().
	wildcard = ident.New("*")
)

type authenticator struct {
	mu struct {
		sync.RWMutex
		publicKeys []crypto.PublicKey
		revoked    map[string]struct{}
	}

	sql struct {
		selectKeys    string
		selectRevoked string
	}
}

var _ types.Authenticator = (*authenticator)(nil)

// Check implements types.Authenticator.
func (a *authenticator) Check(
	_ context.Context, schema ident.Schema, token string,
) (ok bool, _ error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	for _, key := range a.mu.publicKeys {
		var claims Claims
		_, err := jwt.ParseWithClaims(token,
			&claims,
			func(unvalidated *jwt.Token) (any, error) {
				return key, nil
			},
			jwt.WithValidMethods(validJWTMethods),
		)
		if err != nil {
			log.WithError(errors.WithStack(err)).Trace("invalid token")
			continue
		}
		if _, revoked := a.mu.revoked[claims.ID]; revoked {
			log.WithFields(log.Fields{
				"id":     claims.ID,
				"schema": schema,
			}).Debug("saw revoked token")
			return false, nil
		}
		for _, allowed := range claims.Ext.Schemas {
			if matches(allowed, schema) {
				log.WithFields(log.Fields{
					"id":     claims.ID,
					"schema": schema,
				}).Debug("successful authorization")
				return true, nil
			}
		}
	}

	return false, nil
}

const (
	ensureKeysTemplate = `
CREATE TABLE IF NOT EXISTS %s (
	id UUID NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
	public_key STRING NOT NULL,
	active BOOL NOT NULL DEFAULT true
)`
	ensureRevokedTemplate = `
CREATE TABLE IF NOT EXISTS %s (
	id STRING NOT NULL PRIMARY KEY
)`
	selectKeysTemplate    = `SELECT id, public_key FROM %s WHERE active`
	selectRevokedTemplate = `SELECT id FROM %s`
)

// refresh will load PEM-encoded public keys from the database table
// as well as a list of revoked JWT token ids.
func (a *authenticator) refresh(ctx context.Context, tx types.StagingQuerier) error {
	var nextKeys []crypto.PublicKey
	nextRevoked := make(map[string]struct{})

	rows, err := tx.Query(ctx, a.sql.selectKeys)
	if err != nil {
		return errors.WithStack(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id uuid.UUID
		var pemBytes []byte
		if err := rows.Scan(&id, &pemBytes); err != nil {
			return errors.WithStack(err)
		}

		// Decode doesn't return an error, just the rest of the bytes.
		block, _ := pem.Decode(pemBytes)
		if block == nil {
			return errors.Errorf("no PEM data in %s", id)
		}

		key, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return errors.Wrapf(err, "could not decode public key in %s", id)
		}
		nextKeys = append(nextKeys, key)
	}
	rows.Close()

	rows, err = tx.Query(ctx, a.sql.selectRevoked)
	if err != nil {
		return errors.WithStack(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return errors.WithStack(err)
		}
		nextRevoked[id] = struct{}{}
	}
	rows.Close()

	a.mu.Lock()
	defer a.mu.Unlock()
	a.mu.publicKeys = nextKeys
	a.mu.revoked = nextRevoked
	log.Trace("refreshed JWT data")
	jwtRefreshedAt.SetToCurrentTime()
	return nil
}

func matches(allowed, requested ident.Schema) bool {
	allowedParts := allowed.Idents(nil)
	requestedParts := requested.Idents(nil)

	for len(allowedParts) > 0 && len(requestedParts) > 0 {
		partMatches := allowedParts[0] == requestedParts[0] || allowedParts[0] == wildcard
		if !partMatches {
			return false
		}
		allowedParts = allowedParts[1:]
		requestedParts = requestedParts[1:]
	}

	return len(allowedParts) == 0 && len(requestedParts) == 0
}
