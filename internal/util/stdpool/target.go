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

package stdpool

import (
	"net/url"
	"strings"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/pkg/errors"
)

// OpenTarget selects from target connector implementations based on the
// URL scheme contained in the connection string.
func OpenTarget(
	ctx *stopper.Context, connectString string, options ...Option,
) (*types.TargetPool, error) {
	u, err := url.Parse(connectString)
	if err != nil {
		return nil, errors.Wrap(err, "could not parse connection string")
	}

	switch strings.ToLower(u.Scheme) {
	case "mysql":
		return OpenMySQLAsTarget(ctx, connectString, u, options...)
	case "pg", "pgx", "postgres", "postgresql":
		return OpenPgxAsTarget(ctx, connectString, options...)
	case "ora", "oracle":
		return OpenOracleAsTarget(ctx, connectString, options...)
	default:
		return nil, errors.Errorf("unknown URL scheme: %s", u.Scheme)
	}
}
