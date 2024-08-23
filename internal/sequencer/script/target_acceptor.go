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

package script

import (
	"context"
	"database/sql"

	"github.com/cockroachdb/replicator/internal/script"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// A targetAcceptor is responsible for the actions wired up to a
// configureTarget() api call. Specifically, the targetAcceptor
// interacts with user-defined apply functions or final data fixups.
type targetAcceptor struct {
	delegate   types.TableAcceptor
	ensureTX   bool
	group      *types.TableGroup
	targetPool *types.TargetPool
	userScript *script.UserScript
}

var _ types.TableAcceptor = (*targetAcceptor)(nil)

// AcceptTableBatch implements [types.TableAcceptor]. It will invoke
// user-defined dispatch and/or map functions.
func (a *targetAcceptor) AcceptTableBatch(
	ctx context.Context, batch *types.TableBatch, opts *types.AcceptOptions,
) error {
	if _, isTX := opts.TargetQuerier.(*sql.Tx); a.ensureTX && !isTX {
		return a.acceptWithTransaction(ctx, batch, opts)
	}
	return a.doMap(ctx, batch, opts)
}

// acceptWithTransaction creates a database transaction and calls
// AcceptTableBatch. This code path is used in immediate mode when the
// userscript has a user-defined accept function callback.
func (a *targetAcceptor) acceptWithTransaction(
	ctx context.Context, batch *types.TableBatch, opts *types.AcceptOptions,
) error {
	log.Trace("creating target transaction for user-defined apply function")
	tx, err := a.targetPool.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() { _ = tx.Rollback() }()

	opts = opts.Copy()
	opts.TargetQuerier = tx

	// Return to our usual dispatch.
	if err := a.AcceptTableBatch(ctx, batch, opts); err != nil {
		return err
	}

	return errors.WithStack(tx.Commit())
}

func (a *targetAcceptor) doMap(
	ctx context.Context, batch *types.TableBatch, opts *types.AcceptOptions,
) error {
	target, ok := a.userScript.Targets.Get(batch.Table)
	if !ok {
		// No target configuration.
		return a.delegate.AcceptTableBatch(ctx, batch, opts)
	}

	if target.Map != nil {
		mapped := batch.Empty()
		mapped.Data = make([]types.Mutation, 0, len(batch.Data))
		for _, mut := range batch.Data {
			if mut.IsDelete() {
				if target.DeleteKey == nil {
					mapped.Data = append(mapped.Data, mut)
				} else {
					next, keep, err := target.DeleteKey(ctx, mut)
					if err != nil {
						return err
					}
					if keep {
						mapped.Data = append(mapped.Data, next)
					}
				}
				continue
			}
			script.AddMeta(a.group.Name.Raw(), batch.Table, &mut)
			next, keep, err := target.Map(ctx, mut)
			if err != nil {
				return err
			}
			if !keep {
				continue
			}
			mapped.Data = append(mapped.Data, next)
		}
		batch = mapped
	}

	// Delegate to user-provided logic. This may wind up delegating to
	// our delegate anyway.
	if acc := target.UserAcceptor; acc != nil {
		return acc.AcceptTableBatch(ctx, batch, opts)
	}

	// Otherwise, continue down the standard path.
	return a.delegate.AcceptTableBatch(ctx, batch, opts)
}
