// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logical

import (
	"context"

	"github.com/cockroachdb/cdc-sink/internal/target/script"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
)

// scriptEvents wraps an Events implementation to allow a user-script
// to intercept and dispatch mutations.
type scriptEvents struct {
	Events
	Script *script.UserScript
}

var _ Events = (*scriptEvents)(nil)

// OnData implements Events and calls any mapping logic provided by the
// user-script for the given table. If there is no configured behavior
// for the given source table
func (e *scriptEvents) OnData(
	ctx context.Context, source ident.Ident, target ident.Table, muts []types.Mutation,
) error {
	cfg, ok := e.Script.Sources[source]
	if !ok {
		return e.sendToTarget(ctx, source, target, muts)
	}

	sourceMapper := cfg.Mapper
	if sourceMapper == nil {
		return e.sendToTarget(ctx, source, target, muts)
	}
	for _, mut := range muts {
		// For deletes, we won't have anything more than the original
		// key, so the best that we can do is to route the deletion to a
		// table and allow FK's to perform the cascade.
		if mut.IsDelete() {
			deletesTo := cfg.DeletesTo
			if deletesTo == (ident.Table{}) {
				return errors.Errorf(
					"cannot apply delete from %s because there is no "+
						"table configured for receiving the delete", source)
			}
			if err := e.Events.OnData(ctx, source, deletesTo, []types.Mutation{mut}); err != nil {
				return err
			}
			continue
		}

		routing, err := sourceMapper(ctx, mut)
		if err != nil {
			return err
		}
		if len(routing) > 0 {
			for dest, muts := range routing {
				if err := e.sendToTarget(ctx, source, dest, muts); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// sendToTarget applies any per-target logic in the user-script and
// then delegates to Events.OnData.
func (e *scriptEvents) sendToTarget(
	ctx context.Context, source ident.Ident, target ident.Table, muts []types.Mutation,
) error {
	cfg, ok := e.Script.Targets[target]
	if !ok {
		return e.Events.OnData(ctx, source, target, muts)
	}
	mapperFn := cfg.Map
	if mapperFn == nil {
		return e.Events.OnData(ctx, source, target, muts)
	}

	// Filter with replacement.
	idx := 0
	for _, mut := range muts {
		mut, ok, err := mapperFn(ctx, mut)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		muts[idx] = mut
		idx++
	}
	if idx == 0 {
		return nil
	}
	muts = muts[:idx]
	return e.Events.OnData(ctx, source, target, muts)
}
