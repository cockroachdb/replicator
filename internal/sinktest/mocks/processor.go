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

package mocks

import (
	"math/rand"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/source/objstore/bucket"
	"github.com/cockroachdb/replicator/internal/source/objstore/eventproc"
	"github.com/cockroachdb/replicator/internal/types"
)

// chaosProcessor wraps a eventproc.Processor and injects transient errors.
type chaosProcessor struct {
	delegate           eventproc.Processor
	probTransientError float32
}

var _ eventproc.Processor = &chaosProcessor{}

// NewChaosProcessor creates a new shim around a processor that throws
// intermittent transient errors.
func NewChaosProcessor(delegate eventproc.Processor, prob float32) eventproc.Processor {
	return &chaosProcessor{
		delegate:           delegate,
		probTransientError: prob,
	}
}

// Process implements eventproc.Processor.
// It may return an ErrTransient to verify retry.
func (c *chaosProcessor) Process(
	ctx *stopper.Context, path string, filters ...types.MutationFilter,
) error {
	if rand.Float32() <= c.probTransientError {
		return bucket.ErrTransient
	}
	return c.delegate.Process(ctx, path, filters...)
}
