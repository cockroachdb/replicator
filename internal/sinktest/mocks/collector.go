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
	"slices"
	"sync"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/source/objstore/eventproc"
	"github.com/cockroachdb/replicator/internal/types"
)

// Collector implements eventproc.Processor. It tracks the files that
// have processed for testing purposes.
type Collector struct {
	mu struct {
		sync.Mutex
		batch []string
	}
}

var _ eventproc.Processor = &Collector{}

// NewCollector builds a processor that tracks the files
// processed from a object store.
func NewCollector(capacity int) *Collector {
	c := &Collector{}
	c.mu.batch = make([]string, 0, capacity)
	return c
}

// GetSorted returns the files collected, in lexicographic order.
func (c *Collector) GetSorted() []string {
	c.mu.Lock()
	tmp := make([]string, len(c.mu.batch))
	copy(tmp, c.mu.batch)
	c.mu.Unlock()
	slices.Sort(tmp)
	return tmp
}

// Process implements eventproc.Processor.
// It may return an ErrTransient to verify retry.
func (c *Collector) Process(
	ctx *stopper.Context, path string, filters ...types.MutationFilter,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.batch = append(c.mu.batch, path)
	return nil
}
