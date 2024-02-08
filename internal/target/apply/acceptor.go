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

package apply

import (
	"context"

	"github.com/cockroachdb/cdc-sink/internal/types"
)

// Acceptor writes batches of mutations into their target tables.
type Acceptor struct {
	factory    *factory
	targetPool *types.TargetPool
}

var _ types.TableAcceptor = (*Acceptor)(nil)

// AcceptTableBatch implements [types.TableAcceptor]. If the options do
// not provide a [types.TargetQuerier], the [types.TargetPool] will be
// used instead.
func (a *Acceptor) AcceptTableBatch(
	ctx context.Context, batch *types.TableBatch, opts *types.AcceptOptions,
) error {
	apply, err := a.factory.Get(ctx, batch.Table)
	if err != nil {
		return err
	}

	tq := opts.TargetQuerier
	if tq == nil {
		tq = a.targetPool
	}
	return apply.Apply(ctx, tq, batch.Data)
}
