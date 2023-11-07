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

package stdserver

import (
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/auth/jwt"
	"github.com/cockroachdb/cdc-sink/internal/util/auth/trust"
	"github.com/cockroachdb/cdc-sink/internal/util/diag"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	log "github.com/sirupsen/logrus"
)

// Authenticator constructs a JWT-based authenticator,
// or a no-op authenticator if Config.DisableAuth has been set.
func Authenticator(
	ctx *stopper.Context,
	diags *diag.Diagnostics,
	config *Config,
	pool *types.StagingPool,
	stagingDB ident.StagingSchema,
) (types.Authenticator, error) {
	var auth types.Authenticator
	var err error
	if config.DisableAuth {
		log.Info("authentication disabled, any caller may write to the target database")
		auth = trust.New()
	} else {
		auth, err = jwt.ProvideAuth(ctx, pool, stagingDB)
	}
	if d, ok := auth.(diag.Diagnostic); ok {
		if err := diags.Register("auth", d); err != nil {
			return nil, err
		}
	}
	return auth, err
}
