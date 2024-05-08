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

// Package reject contains a types.Authenticator which always returns
// false.
package reject

import (
	"context"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/ident"
)

// authenticator denies all requests.
type authenticator struct{}

// New returns an Authenticator which denies all requests.
func New() types.Authenticator {
	return &authenticator{}
}

// Check always returns false.
func (a *authenticator) Check(context.Context, ident.Schema, string) (bool, error) {
	return false, nil
}
