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

package decorators

import (
	"context"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/retry"
	"github.com/pkg/errors"
)

// RetryTarget adds a retry loop around delegates that interact with the
// target database.
type RetryTarget struct {
	targetPool *types.TargetPool
}

// MultiAcceptor returns a retrying facade around the delegate.
func (r *RetryTarget) MultiAcceptor(acceptor types.MultiAcceptor) types.MultiAcceptor {
	return &retryTarget{
		base: base{
			multiAcceptor:    acceptor,
			tableAcceptor:    acceptor,
			temporalAcceptor: acceptor,
		},
		RetryTarget: r,
	}
}

// TableAcceptor returns a retrying facade around the delegate.
func (r *RetryTarget) TableAcceptor(acceptor types.TableAcceptor) types.TableAcceptor {
	return &retryTarget{
		base: base{
			tableAcceptor: acceptor,
		},
		RetryTarget: r,
	}
}

// TemporalAcceptor returns a retrying facade around the delegate.
func (r *RetryTarget) TemporalAcceptor(acceptor types.TemporalAcceptor) types.TemporalAcceptor {
	return &retryTarget{
		base: base{
			tableAcceptor:    acceptor,
			temporalAcceptor: acceptor,
		},
		RetryTarget: r,
	}
}

type retryTarget struct {
	base
	*RetryTarget
}

var _ types.MultiAcceptor = (*retryTarget)(nil)

func (r *retryTarget) AcceptMultiBatch(
	ctx context.Context, batch *types.MultiBatch, opts *types.AcceptOptions,
) error {
	if r.multiAcceptor == nil {
		return errors.New("no multiAcceptor set")
	}
	return retry.Retry(ctx, r.targetPool, func(ctx context.Context) error {
		return r.multiAcceptor.AcceptMultiBatch(ctx, batch, opts)
	})
}

func (r *retryTarget) AcceptTableBatch(
	ctx context.Context, batch *types.TableBatch, opts *types.AcceptOptions,
) error {
	if r.multiAcceptor == nil {
		return errors.New("no multiAcceptor set")
	}
	return retry.Retry(ctx, r.targetPool, func(ctx context.Context) error {
		return r.multiAcceptor.AcceptTableBatch(ctx, batch, opts)
	})
}

func (r *retryTarget) AcceptTemporalBatch(
	ctx context.Context, batch *types.TemporalBatch, opts *types.AcceptOptions,
) error {
	if r.temporalAcceptor == nil {
		return errors.New("no temporalAcceptor set")
	}
	return retry.Retry(ctx, r.targetPool, func(ctx context.Context) error {
		return r.temporalAcceptor.AcceptTemporalBatch(ctx, batch, opts)
	})
}
