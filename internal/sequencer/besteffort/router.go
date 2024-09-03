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

package besteffort

import (
	"context"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/pkg/errors"
)

// A router sends mutations to the sequencer associated with a target
// table.
type router struct {
	routes ident.TableMap[types.TableAcceptor]
}

var _ types.TableAcceptor = (*router)(nil)

func (r *router) AcceptTableBatch(
	ctx context.Context, batch *types.TableBatch, opts *types.AcceptOptions,
) error {
	dest, ok := r.routes.Get(batch.Table)
	if !ok {
		return errors.Errorf("unknown table %s", batch.Table)
	}
	return dest.AcceptTableBatch(ctx, batch, opts)
}
