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

package objstore

import (
	"context"

	"github.com/cockroachdb/replicator/internal/conveyor"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
)

// Conveyor exposes the methods used by the object store connector to deliver
// mutations, in batches, to the destination. It controls the checkpoint
// timestamp associated to each partition.
type Conveyor interface {
	// AcceptMultiBatch processes a batch. The batch is committed to the target
	// database or to a staging area, depending on the mode in which
	// the connector is running.
	AcceptMultiBatch(context.Context, *types.MultiBatch, *types.AcceptOptions) error
	// Advance extends the proposed checkpoint timestamp associated with a partition.
	// It is called when a resolved timestamp is received by the consumer.
	Advance(context.Context, ident.Ident, hlc.Time) error
	// Ensure that a checkpoint exists for all the given partitions. It should be
	// called every time a new partition or topic is discovered by the consumer group.
	Ensure(context.Context, []ident.Ident) error
	// Access to the underlying schema.
	Watcher() types.Watcher
}

// We make sure that the concrete conveyor.Conveyor implements the Conveyor interface.
var _ Conveyor = &conveyor.Conveyor{}
