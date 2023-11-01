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

package cdc

import (
	"context"
	"encoding/json"
	"io"

	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/pkg/errors"
)

// webhook responds to the v21.2 webhook scheme.
// https://www.cockroachlabs.com/docs/stable/create-changefeed.html#responses
func (h *Handler) webhook(ctx context.Context, req *request) error {
	var payload struct {
		Payload []struct {
			After   json.RawMessage `json:"after"`
			Before  json.RawMessage `json:"before"`
			Key     json.RawMessage `json:"key"`
			Topic   string          `json:"topic"`
			Updated string          `json:"updated"`
		} `json:"payload"`
		Length   int    `json:"length"`
		Resolved string `json:"resolved"`
	}
	dec := json.NewDecoder(req.body)
	dec.DisallowUnknownFields()
	dec.UseNumber()
	if err := dec.Decode(&payload); err != nil {
		// Empty input is a no-op.
		if errors.Is(err, io.EOF) {
			return nil
		}
		return errors.Wrap(err, "could not decode payload")
	}
	target := req.target.(ident.Schema)
	if payload.Resolved != "" {
		timestamp, err := hlc.Parse(payload.Resolved)
		if err != nil {
			return err
		}
		req.timestamp = timestamp

		return h.resolved(ctx, req)
	}

	// Aggregate the mutations by target table. We know that the default
	// batch size for webhooks is reasonable.
	toProcess := &ident.TableMap[[]types.Mutation]{}

	for i := range payload.Payload {
		timestamp, err := hlc.Parse(payload.Payload[i].Updated)
		if err != nil {
			return err
		}

		table, qual, err := ident.ParseTableRelative(payload.Payload[i].Topic, target)
		if err != nil {
			return err
		}
		// Ensure the destination table is in the target schema.
		if qual != ident.TableOnly {
			table = ident.NewTable(target, table.Table())
		}

		mut := types.Mutation{
			Before: payload.Payload[i].Before,
			Data:   payload.Payload[i].After,
			Key:    payload.Payload[i].Key,
			Time:   timestamp,
		}
		toProcess.Put(table, append(toProcess.GetZero(table), mut))
	}
	if h.Config.Immediate {
		return h.processMutationsImmediate(ctx, target, toProcess)
	}
	return h.processMutationsDeferred(ctx, toProcess)
}

func (h *Handler) processMutationsDeferred(
	ctx context.Context, toProcess *ident.TableMap[[]types.Mutation],
) error {
	// Create Store instances up front. The first time a target table is
	// used, the Stager must create the staging table. We want to ensure
	// that this happens before we create the transaction below.
	stores := &ident.TableMap[types.Stager]{}
	if err := toProcess.Range(func(table ident.Table, _ []types.Mutation) error {
		s, err := h.Stores.Get(ctx, table)
		if err != nil {
			return err
		}
		stores.Put(table, s)
		return nil
	}); err != nil {
		return err
	}

	return retry.Retry(ctx, func(ctx context.Context) error {
		pool := h.StagingPool.Pool
		tx, err := pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		// Stage or apply the per-target mutations.
		if err := toProcess.Range(func(target ident.Table, muts []types.Mutation) error {
			return stores.GetZero(target).Store(ctx, tx, muts)
		}); err != nil {
			return err
		}

		return tx.Commit(ctx)
	})
}
func (h *Handler) processMutationsImmediate(
	ctx context.Context, target ident.Schema, toProcess *ident.TableMap[[]types.Mutation],
) error {
	batcher, err := h.Immediate.Get(target)
	if err != nil {
		return err
	}

	batch, err := batcher.OnBegin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = batch.OnRollback(ctx) }()

	source := script.SourceName(target)
	if err := toProcess.Range(func(tbl ident.Table, muts []types.Mutation) error {
		for idx := range muts {
			// Index needed since it's not a pointer type.
			script.AddMeta("cdc", tbl, &muts[idx])
		}
		return batch.OnData(ctx, source, tbl, muts)
	}); err != nil {
		return err
	}

	select {
	case err := <-batch.OnCommit(ctx):
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
