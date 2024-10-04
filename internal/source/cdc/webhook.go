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

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/pkg/errors"
)

// WebhookPayloadLine is an encoding of a single mutation.
type WebhookPayloadLine struct {
	After   json.RawMessage `json:"after"`
	Before  json.RawMessage `json:"before"`
	Key     json.RawMessage `json:"key"`
	Topic   string          `json:"topic"`
	Updated string          `json:"updated"`
}

// WebhookPayload describes the envelope structure that we expect to see
// from a CockroachDB webhook changefeed.
type WebhookPayload struct {
	Payload  []WebhookPayloadLine `json:"payload"`
	Length   int                  `json:"length"`
	Resolved string               `json:"resolved"`

	// This field is populated by [NewWebhookPayload]. It is not part
	// of any actual payload, but is useful when synthesizing workloads.
	Range hlc.Range `json:"-"`
}

// NewWebhookPayload constructs a payload for the batch.
func NewWebhookPayload(batch *types.MultiBatch) (*WebhookPayload, error) {
	ret := &WebhookPayload{}
	// Ignoring error since callback returns nil.
	if err := batch.CopyInto(types.AccumulatorFunc(func(table ident.Table, mut types.Mutation) error {
		line := WebhookPayloadLine{
			After:   mut.Data,
			Before:  mut.Before,
			Key:     mut.Key,
			Topic:   table.Raw(),
			Updated: mut.Time.String(),
		}
		// Webhook encodes a deletion as an absence of an after block.
		if mut.IsDelete() {
			line.After = nil
		}
		ret.Payload = append(ret.Payload, line)
		ret.Range = ret.Range.Extend(mut.Time)
		return nil
	})); err != nil {
		return nil, err
	}
	ret.Length = len(ret.Payload)
	return ret, nil
}

// NewWebhookResolved constructs
func NewWebhookResolved(ts hlc.Time) (*WebhookPayload, error) {
	return &WebhookPayload{Resolved: ts.String()}, nil
}

// webhook responds to the v21.2 webhook scheme.
// https://www.cockroachlabs.com/docs/stable/create-changefeed.html#responses
func (h *Handler) webhook(ctx context.Context, req *request) error {
	var payload WebhookPayload
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
	conveyor, err := h.Conveyors.Get(req.target.Schema())
	if err != nil {
		return err
	}

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
	toProcess := &types.MultiBatch{}

	for i := range payload.Payload {
		timestamp, err := hlc.Parse(payload.Payload[i].Updated)
		if err != nil {
			return err
		}

		// In the webhook case, if the payload topic is that means the table was
		// not passed in properly.
		if payload.Payload[i].Topic == "" {
			return errors.New("table name is empty, please ensure the table name is included in the path")
		}

		table, qual, err := ident.ParseTableRelative(payload.Payload[i].Topic, req.target.Schema())
		if err != nil {
			return err
		}
		// Ensure the destination table is in the target schema.
		if qual != ident.TableOnly {
			table = ident.NewTable(req.target.Schema(), table.Table())
		}

		mut := types.Mutation{
			Before: payload.Payload[i].Before,
			Data:   payload.Payload[i].After,
			Key:    payload.Payload[i].Key,
			Time:   timestamp,
		}
		if err := toProcess.Accumulate(table, mut); err != nil {
			return err
		}
	}

	return conveyor.AcceptMultiBatch(ctx, toProcess, &types.AcceptOptions{})
}
