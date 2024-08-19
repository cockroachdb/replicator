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

// Package workload contains utility types for creating synthetic
// workloads. The types in this package should avoid direct database
// access.
package workload

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	log "github.com/sirupsen/logrus"
)

// A ChildRow contains an expected snapshot of some child table row.
type ChildRow struct {
	ID     int
	Parent int
	Val    int64
}

// A ParentRow contains an expected snapshot of some parent table row.
type ParentRow struct {
	ID  int
	Val int64
}

// GeneratorBase creates batches of test data. It assumes that there is
// a parent table and a child table.
type GeneratorBase struct {
	// Tables and expected rows.
	Child    ident.Table      `json:"child"`
	Children map[int]struct{} `json:"children"`
	Parent   ident.Table      `json:"parent"`
	Parents  map[int]struct{} `json:"parents"`

	// The highest timestamp passed to GenerateInto.
	MaxTime hlc.Time `json:"maxTime"`

	// Compare data to ensure time order is maintained.
	ChildVals  map[int]int64 `json:"childVals"`
	ParentVals map[int]int64 `json:"parentVals"`

	// The keys are child ids and the value the parent id.
	ChildToParent map[int]int `json:"childToParent"`

	// generation tracks the number of times that [GenerateInto] has
	// been called in order to select different scenarios.
	generation int
}

// NewGeneratorBase constructs an empty payload generator.
func NewGeneratorBase(parent, child ident.Table) *GeneratorBase {
	return &GeneratorBase{
		Child:         child,
		Children:      make(map[int]struct{}),
		ChildToParent: make(map[int]int),
		ChildVals:     make(map[int]int64),
		Parent:        parent,
		Parents:       make(map[int]struct{}),
		ParentVals:    make(map[int]int64),
	}
}

// ChildRows returns a summary of all expected child table rows.
func (g *GeneratorBase) ChildRows() []*ChildRow {
	ret := make([]*ChildRow, 0, len(g.Parents))
	for child := range g.Children {
		ret = append(ret, &ChildRow{
			ID:     child,
			Parent: g.ChildToParent[child],
			Val:    g.ChildVals[child],
		})
	}
	return ret
}

// GenerateInto will add mutations to the batch at the requested time.
func (g *GeneratorBase) GenerateInto(batch *types.MultiBatch, time hlc.Time) {
	val := time.Nanos()
	switch g.generation % 7 {
	case 0: // Insert a parent row
		parent := g.pickNewParent()
		g.ParentVals[parent] = val
		_ = batch.Accumulate(g.Parent, types.Mutation{
			Data: json.RawMessage(fmt.Sprintf(`{ "parent": %d, "val": %d}`, parent, val)),
			Key:  json.RawMessage(fmt.Sprintf(`[ %d ]`, parent)),
			Time: time,
		})

	case 1: // Update a parent row
		parent := g.pickExistingParent()
		g.ParentVals[parent] = val
		_ = batch.Accumulate(g.Parent, types.Mutation{
			Data: json.RawMessage(fmt.Sprintf(`{ "parent": %d, "val": %d }`, parent, val)),
			Key:  json.RawMessage(fmt.Sprintf(`[ %d ]`, parent)),
			Time: time,
		})

	case 2: // Insert a child row referencing an existing parent
		parent := g.pickExistingParent()
		child := g.pickNewChild()
		g.ChildVals[child] = val
		g.ChildToParent[child] = parent
		_ = batch.Accumulate(g.Child, types.Mutation{
			Data: json.RawMessage(fmt.Sprintf(`{ "child": %d, "parent": %d, "val": %d }`,
				child, parent, val)),
			Key:  json.RawMessage(fmt.Sprintf(`[ %d ]`, child)),
			Time: time,
		})

	case 3: // Insert a new child row referencing a new parent
		parent := g.pickNewParent()
		_ = batch.Accumulate(g.Parent, types.Mutation{
			Data: json.RawMessage(fmt.Sprintf(`{ "parent": %d, "val": %d }`, parent, val)),
			Key:  json.RawMessage(fmt.Sprintf(`[ %d ]`, parent)),
			Time: time,
		})
		g.ParentVals[parent] = val

	case 4: // Re-parent an existing child
		parent := g.pickExistingParent()
		child := g.pickExistingChild()
		g.ChildVals[child] = val
		g.ChildToParent[child] = parent
		_ = batch.Accumulate(g.Child, types.Mutation{
			Data: json.RawMessage(fmt.Sprintf(`{ "child": %d, "parent": %d, "val": %d }`,
				child, parent, val)),
			Key:  json.RawMessage(fmt.Sprintf(`[ %d ]`, child)),
			Time: time,
		})

	case 5: // Delete a child
		child := g.pickExistingChild()
		delete(g.Children, child)
		delete(g.ChildToParent, child)
		delete(g.ChildVals, child)
		_ = batch.Accumulate(g.Child, types.Mutation{
			Key:  json.RawMessage(fmt.Sprintf(`[ %d ]`, child)),
			Time: time,
		})

	case 6: // Delete a parent
		parent := g.pickExistingParent()
		delete(g.Parents, parent)
		delete(g.ParentVals, parent)
		_ = batch.Accumulate(g.Parent, types.Mutation{
			Key:  json.RawMessage(fmt.Sprintf(`[ %d ]`, parent)),
			Time: time,
		})

		// Also cascade to children.
		for child, p := range g.ChildToParent {
			if p != parent {
				continue
			}
			delete(g.Children, child)
			delete(g.ChildToParent, child)
			delete(g.ChildVals, child)
			_ = batch.Accumulate(g.Child, types.Mutation{
				Key:  json.RawMessage(fmt.Sprintf(`[ %d ]`, child)),
				Time: time,
			})
		}

	default:
		panic("check your modulus")
	}

	g.generation++
	if hlc.Compare(time, g.MaxTime) > 0 {
		g.MaxTime = time
	}
}

// ParentRows returns a summary of all expected parent table rows.
func (g *GeneratorBase) ParentRows() []*ParentRow {
	ret := make([]*ParentRow, 0, len(g.Parents))
	for parent := range g.Parents {
		ret = append(ret, &ParentRow{
			ID:  parent,
			Val: g.ParentVals[parent],
		})
	}
	return ret
}

// Range returns a range that includes all times at which the Generator
// created data.
func (g *GeneratorBase) Range() hlc.Range {
	return hlc.RangeIncluding(hlc.Zero(), g.MaxTime)
}

// WaitForCatchUp returns when the Stat shows all tables have advanced
// beyond the highest timestamp passed to [Generator.GenerateInto].
func (g *GeneratorBase) WaitForCatchUp(
	ctx context.Context, stats *notify.Var[sequencer.Stat],
) error {
	for {
		stat, changed := stats.Get()
		progress := sequencer.CommonProgress(stat)
		if hlc.Compare(progress.Max(), g.MaxTime) >= 0 {
			log.Debugf("caught up to %s", progress)
			return nil
		}
		log.Debugf("waiting for catch-up: %s vs %s", progress, g.MaxTime)
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (g *GeneratorBase) pickExistingChild() int {
	// Rely on random iteration order.
	for child := range g.Children {
		return child
	}
	panic("no children")
}

func (g *GeneratorBase) pickExistingParent() int {
	// Rely on random iteration order.
	for parent := range g.Parents {
		return parent
	}
	panic("no parents")
}

func (g *GeneratorBase) pickNewChild() int {
	for {
		child := int(rand.Int31())
		if _, exists := g.Children[child]; exists {
			continue
		}
		g.Children[child] = struct{}{}
		return child
	}
}

func (g *GeneratorBase) pickNewParent() int {
	for {
		parent := int(rand.Int31())
		if _, exists := g.Parents[parent]; exists {
			continue
		}
		g.Parents[parent] = struct{}{}
		return parent
	}
}
