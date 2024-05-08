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

package immediate

import (
	"context"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/retry"
)

// acceptor adds a retry loop around a delegate. This provides better
// insulation of callers against database contention.
type acceptor struct {
	*Immediate

	delegate types.MultiAcceptor
}

var _ types.MultiAcceptor = (*acceptor)(nil)

// AcceptTableBatch adds a retry loop around the delegate.
func (a *acceptor) AcceptTableBatch(
	ctx context.Context, batch *types.TableBatch, opts *types.AcceptOptions,
) error {
	return retry.Retry(ctx, a.targetPool, func(ctx context.Context) error {
		return a.delegate.AcceptTableBatch(ctx, batch, opts)
	})
}

// AcceptTemporalBatch adds a retry loop around the delegate.
func (a *acceptor) AcceptTemporalBatch(
	ctx context.Context, batch *types.TemporalBatch, opts *types.AcceptOptions,
) error {
	return retry.Retry(ctx, a.targetPool, func(ctx context.Context) error {
		return a.delegate.AcceptTemporalBatch(ctx, batch, opts)
	})
}

// AcceptMultiBatch adds a retry loop around the delegate.
func (a *acceptor) AcceptMultiBatch(
	ctx context.Context, batch *types.MultiBatch, opts *types.AcceptOptions,
) error {
	return retry.Retry(ctx, a.targetPool, func(ctx context.Context) error {
		return a.delegate.AcceptMultiBatch(ctx, batch, opts)
	})
}
