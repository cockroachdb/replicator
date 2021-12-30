// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package trust contains a types.Authenticator which always returns true.
package trust

import (
	"context"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
)

// authenticator is a no-op implementation of types.Authenticator
// that always returns true.
type authenticator struct{}

// New returns an Authenticator which always allows incoming requests.
func New() types.Authenticator {
	return &authenticator{}
}

var _ types.Authenticator = (*authenticator)(nil)

// Check always returns true.
func (a *authenticator) Check(context.Context, ident.Schema, string) (bool, error) {
	return true, nil
}
