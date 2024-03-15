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

package core

import (
	"fmt"
	"sync"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
)

// errPoisoned is a sentinel value used to short-circuit execution.
var errPoisoned = errors.New("poisoned")

type poisonedKey string

func poisonKey(table ident.Table, mut types.Mutation) poisonedKey {
	return poisonedKey(fmt.Sprintf("%s:%s", table.Raw(), mut.Key))
}

// poisonSet is a concurrency-safe helper to manage poisoned keys.
type poisonSet struct {
	// maxCount sets a threshold value where all keys are considered
	// poisoned. This enables a graceful backoff and retry in FK schemas
	// to try to get a child table applier loop to "run behind" the
	// applier loop for the parent table. It also places an upper bound
	// on the number of elements in the data map.
	maxCount int

	mu struct {
		sync.RWMutex
		data map[poisonedKey]struct{}
		full bool
	}
}

func newPoisonSet(maxCount int) *poisonSet {
	ret := &poisonSet{maxCount: maxCount}
	ret.mu.data = make(map[poisonedKey]struct{}, maxCount)
	return ret
}

// ForceFull ensures that all future batches will be poisoned.
func (p *poisonSet) ForceFull() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.full = true
}

// IsClean returns true if no keys were poisoned.
func (p *poisonSet) IsClean() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return !p.mu.full && len(p.mu.data) == 0
}

// IsFull returns true if the upper bound on the number of poisoned keys
// was hit.
func (p *poisonSet) IsFull() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.mu.full
}

// IsPoisoned returns true if the maximum number of poisoned keys has
// been hit or if the batch contains any poisoned keys. Poisoning is
// contagious. If the batch contains any keys that were not already
// poisoned, they will be poisoned as well. This ensures that
// temporal consistency dependencies are maintained.
func (p *poisonSet) IsPoisoned(batch *types.MultiBatch) bool {
	// Read-locked check.
	toContaminate, isPoisoned := p.toPoison(batch)
	if !isPoisoned {
		return false
	}

	// Additional keys to mark as poisoned.
	if len(toContaminate) > 0 {
		p.mu.Lock()
		defer p.mu.Unlock()
		if !p.mu.full {
			for key := range toContaminate {
				p.mu.data[key] = struct{}{}
			}
			p.mu.full = len(p.mu.data) >= p.maxCount
		}
	}

	return true
}

// toPoison returns a set of keys to contaminate or false if there are
// no poisoned keys.
func (p *poisonSet) toPoison(
	batch *types.MultiBatch,
) (toContaminate map[poisonedKey]struct{}, isPoisoned bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Fast exit if terminal.
	if p.mu.full {
		return nil, true
	}

	// Fast exit if clean.
	if len(p.mu.data) == 0 {
		return nil, false
	}

	// Accumulate keys which may need to be poisoned.
	toContaminate = make(map[poisonedKey]struct{})

	_ = batch.CopyInto(types.AccumulatorFunc(func(table ident.Table, mut types.Mutation) error {
		key := poisonKey(table, mut)
		if _, poisoned := p.mu.data[key]; poisoned {
			isPoisoned = true
		} else {
			toContaminate[key] = struct{}{}
		}
		return nil
	}))

	// No keys within the batch intersect the already-poisoned keys.
	if !isPoisoned {
		return nil, false
	}

	return toContaminate, true
}

// MarkPoisoned records the keys in the batch to prevent any other
// attempts at processing them.
func (p *poisonSet) MarkPoisoned(batch *types.MultiBatch) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.mu.full {
		return
	}

	_ = batch.CopyInto(types.AccumulatorFunc(func(table ident.Table, mut types.Mutation) error {
		p.mu.data[poisonKey(table, mut)] = struct{}{}
		return nil
	}))
	p.mu.full = len(p.mu.data) >= p.maxCount
	if p.mu.full {
		p.mu.data = nil
	}
}
