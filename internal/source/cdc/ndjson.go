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

// This file contains code repackaged from url.go.

import (
	"context"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/cdcjson"
	"github.com/cockroachdb/replicator/internal/util/ident"
)

func (h *Handler) ndjson(
	ctx context.Context, req *request, mutParser cdcjson.MutationReader,
) error {
	table := req.target.(ident.Table)
	batch, err := h.NDJsonParser.Parse(table, mutParser, req.body)
	if err != nil {
		return err
	}
	conveyor, err := h.Conveyors.Get(table.Schema())
	if err != nil {
		return err
	}
	return conveyor.AcceptMultiBatch(ctx, batch, &types.AcceptOptions{})
}
