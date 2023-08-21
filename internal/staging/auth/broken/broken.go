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

// Package broken contains an Authenticator that always fails.
package broken

import (
	"context"
	"errors"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
)

// ErrBroken can be expected by tests.
var ErrBroken = errors.New("broken")

// New returns an Authenticator that always returns [ErrBroken].
func New() types.Authenticator {
	return &broken{}
}

type broken struct{}

func (b *broken) Check(context.Context, ident.Schema, string) (ok bool, _ error) {
	return false, ErrBroken
}
