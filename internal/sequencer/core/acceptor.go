// Copyright 2024 The Cockroach Authors
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

package core

import (
	"context"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/pkg/errors"
)

// acceptor rejects all incoming data since core only operates from a
// BatchReader source.
type acceptor struct{}

var _ types.MultiAcceptor = (*acceptor)(nil)

// AcceptMultiBatch returns an error.
func (a *acceptor) AcceptMultiBatch(
	context.Context, *types.MultiBatch, *types.AcceptOptions,
) error {
	return errors.New("use BatchReader instead")
}

// AcceptTableBatch returns an error.
func (a *acceptor) AcceptTableBatch(
	context.Context, *types.TableBatch, *types.AcceptOptions,
) error {
	return errors.New("use BatchReader instead")
}

// AcceptTemporalBatch returns an error.
func (a *acceptor) AcceptTemporalBatch(
	context.Context, *types.TemporalBatch, *types.AcceptOptions,
) error {
	return errors.New("use BatchReader instead")
}
