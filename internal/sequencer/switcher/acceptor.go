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

package switcher

import (
	"context"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/pkg/errors"
)

// acceptor delegates to the "current" acceptor. Each delegate method
// holds a read lock on the groupSequencer to block acceptance during
// transitions between modes. It also prevents the underlying sequencer
// from being swapped out while in the middle of performing work.
type acceptor struct {
	*groupSequencer
}

var _ types.MultiAcceptor = (*acceptor)(nil)

// AcceptMultiBatch implements [types.MultiAcceptor].
func (s *acceptor) AcceptMultiBatch(
	ctx context.Context, batch *types.MultiBatch, opts *types.AcceptOptions,
) error {
	if batch.Count() == 0 {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	acc := s.mu.acceptor
	if acc == nil {
		return errors.New("switcher has been stopped")
	}
	return acc.AcceptMultiBatch(ctx, batch, opts)
}

// AcceptTemporalBatch implements [types.MultiAcceptor].
func (s *acceptor) AcceptTemporalBatch(
	ctx context.Context, batch *types.TemporalBatch, opts *types.AcceptOptions,
) error {
	if batch.Count() == 0 {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	acc := s.mu.acceptor
	if acc == nil {
		return errors.New("switcher has been stopped")
	}
	return acc.AcceptTemporalBatch(ctx, batch, opts)
}

// AcceptTableBatch implements [types.TableAcceptor].
func (s *acceptor) AcceptTableBatch(
	ctx context.Context, batch *types.TableBatch, opts *types.AcceptOptions,
) error {
	if batch.Count() == 0 {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	acc := s.mu.acceptor
	if acc == nil {
		return errors.New("switcher has been stopped")
	}
	return acc.AcceptTableBatch(ctx, batch, opts)
}

// Unwrap is an informal protocol to return the delegate.
func (s *acceptor) Unwrap() types.MultiAcceptor {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mu.acceptor
}
