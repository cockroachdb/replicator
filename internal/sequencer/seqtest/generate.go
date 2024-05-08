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

package seqtest

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/sinktest/all"
	"github.com/cockroachdb/replicator/internal/sinktest/base"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/notify"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// Generator creates batches of test data. It assumes that there is a
// parent table and a child table. The parents map will be updated
// whenever a new parent entry is required.
type Generator struct {
	Fixture           *all.Fixture
	MaxTime           hlc.Time
	Parent, Child     base.TableInfo[*types.TargetPool]
	Parents, Children map[int]struct{}

	// Compare data to ensure time order is maintained.
	ParentVals, ChildVals map[int]int64

	// The keys are child ids and the value the parent id.
	ChildToParent map[int]int

	ctr int
}

// NewGenerator constructs a Generator that will create messages
// for parent and child tables.
func NewGenerator(
	ctx context.Context, fixture *all.Fixture,
) (*Generator, *types.TableGroup, error) {
	parentInfo, err := fixture.CreateTargetTable(ctx,
		"CREATE TABLE %s (parent INT PRIMARY KEY, val INT DEFAULT 0 NOT NULL)")
	if err != nil {
		return nil, nil, err
	}

	childInfo, err := fixture.CreateTargetTable(ctx, fmt.Sprintf(
		`CREATE TABLE %%s (
child INT PRIMARY KEY,
parent INT NOT NULL,
val INT DEFAULT 0 NOT NULL,
CONSTRAINT parent_fk FOREIGN KEY(parent) REFERENCES %s(parent)
)`, parentInfo.Name()))
	if err != nil {
		return nil, nil, err
	}

	return &Generator{
			Child:         childInfo,
			Children:      make(map[int]struct{}),
			ChildToParent: make(map[int]int),
			ChildVals:     make(map[int]int64),
			Fixture:       fixture,
			Parent:        parentInfo,
			Parents:       make(map[int]struct{}),
			ParentVals:    make(map[int]int64),
		},
		&types.TableGroup{
			Name:      ident.New("testing"),
			Enclosing: fixture.TargetSchema.Schema(),
			Tables:    []ident.Table{childInfo.Name(), parentInfo.Name()},
		},
		nil
}

// CheckConsistent verifies that the staging tables are empty and that
// the requisite number of rows exist in the target tables.
func (g *Generator) CheckConsistent(ctx context.Context, t testing.TB) {
	a := assert.New(t)

	// Verify all mutations have been unstaged.
	rng := g.Range()
	staged, err := g.Fixture.PeekStaged(ctx, g.Parent.Name(), rng)
	if a.NoError(err) {
		a.Emptyf(staged, "staging table %s not empty", g.Parent)
	}
	staged, err = g.Fixture.PeekStaged(ctx, g.Child.Name(), rng)
	if a.NoError(err) {
		a.Emptyf(staged, "staging table %s not empty", g.Child)
	}

	// Verify target row counts against generated data.
	parentCount, err := g.Parent.RowCount(ctx)
	if a.NoError(err) {
		a.Equalf(len(g.Parents), parentCount, "parent %s", g.Parent.Name())
	}
	childCount, err := g.Child.RowCount(ctx)
	if a.NoError(err) {
		a.Equal(len(g.Children), childCount, "child %s", g.Child.Name())
	}

	// Verify parent vals to ensure update order is maintained.
	rows, err := g.Fixture.TargetPool.QueryContext(ctx, fmt.Sprintf(
		"SELECT parent, val FROM %s", g.Parent.Name()))
	if a.NoError(err) {
		seen := 0
		for rows.Next() {
			seen++
			var parent int
			var val int64
			if a.NoError(rows.Scan(&parent, &val)) {
				expectedVal, ok := g.ParentVals[parent]
				if a.Truef(ok, "unexpected parent %d", parent) {
					a.Equalf(expectedVal, val, "parent %d val mismatch", parent)
				}
			}
		}
		// Check final error state.
		if a.NoError(rows.Err()) {
			a.Equal(parentCount, seen)
		}
	}
	_ = rows.Close()

	// Verify child parents and vals to ensure update order.
	rows, err = g.Fixture.TargetPool.QueryContext(ctx, fmt.Sprintf(
		"SELECT child, parent, val FROM %s", g.Child.Name()))
	if a.NoError(err) {
		seen := 0
		for rows.Next() {
			seen++
			var child, parent int
			var val int64
			if a.NoError(rows.Scan(&child, &parent, &val)) {
				expectedVal, ok := g.ChildVals[child]
				if a.Truef(ok, "unexpected child %d", child) {
					a.Equalf(expectedVal, val, "child %d val mismatch", child)
				}
				expectedParent, ok := g.ChildToParent[child]
				if a.Truef(ok, "unexpected child %d", child) {
					a.Equalf(expectedParent, parent, "child %d parent mismatch", child)
				}
			}
		}
		// Check final error state.
		if a.NoError(rows.Err()) {
			a.Equal(childCount, seen)
		}
	}
	_ = rows.Close()
}

// GenerateInto will add mutations to the batch at the requested time.
func (g *Generator) GenerateInto(batch *types.MultiBatch, time hlc.Time) {
	val := time.Nanos()
	switch g.ctr % 5 {
	case 0: // Insert a parent row
		parent := g.pickNewParent()
		g.ParentVals[parent] = val
		_ = batch.Accumulate(g.Parent.Name(), types.Mutation{
			Data: json.RawMessage(fmt.Sprintf(`{ "parent": %d, "val": %d}`, parent, val)),
			Key:  json.RawMessage(fmt.Sprintf(`[ %d ]`, parent)),
			Time: time,
		})

	case 1: // Update a parent row
		parent := g.pickExistingParent()
		g.ParentVals[parent] = val
		_ = batch.Accumulate(g.Parent.Name(), types.Mutation{
			Data: json.RawMessage(fmt.Sprintf(`{ "parent": %d, "val": %d }`, parent, val)),
			Key:  json.RawMessage(fmt.Sprintf(`[ %d ]`, parent)),
			Time: time,
		})

	case 2: // Insert a child row referencing an existing parent
		parent := g.pickExistingParent()
		child := g.pickNewChild()
		g.ChildVals[child] = val
		g.ChildToParent[child] = parent
		_ = batch.Accumulate(g.Child.Name(), types.Mutation{
			Data: json.RawMessage(fmt.Sprintf(`{ "child": %d, "parent": %d, "val": %d }`,
				child, parent, val)),
			Key:  json.RawMessage(fmt.Sprintf(`[ %d ]`, child)),
			Time: time,
		})

	case 3: // Insert a new child row referencing a new parent
		parent := g.pickNewParent()
		_ = batch.Accumulate(g.Parent.Name(), types.Mutation{
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
		_ = batch.Accumulate(g.Child.Name(), types.Mutation{
			Data: json.RawMessage(fmt.Sprintf(`{ "child": %d, "parent": %d, "val": %d }`,
				child, parent, val)),
			Key:  json.RawMessage(fmt.Sprintf(`[ %d ]`, child)),
			Time: time,
		})

	default:
		panic("check your modulus")
	}

	g.ctr++
	if hlc.Compare(time, g.MaxTime) > 0 {
		g.MaxTime = time
	}
}

// Range returns a range that includes all times at which the Generator
// created data.
func (g *Generator) Range() hlc.Range {
	return hlc.RangeIncluding(hlc.Zero(), g.MaxTime)
}

// WaitForCatchUp returns when the Stat shows all tables have advanced
// beyond the highest timestamp passed to [Generator.GenerateInto].
func (g *Generator) WaitForCatchUp(ctx context.Context, stats *notify.Var[sequencer.Stat]) error {
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

func (g *Generator) pickExistingChild() int {
	// Rely on random iteration order.
	for child := range g.Children {
		return child
	}
	panic("no children")
}

func (g *Generator) pickExistingParent() int {
	// Rely on random iteration order.
	for parent := range g.Parents {
		return parent
	}
	panic("no parents")
}

func (g *Generator) pickNewChild() int {
	for {
		child := int(rand.Int31())
		if _, exists := g.Children[child]; exists {
			continue
		}
		g.Children[child] = struct{}{}
		return child
	}
}

func (g *Generator) pickNewParent() int {
	for {
		parent := int(rand.Int31())
		if _, exists := g.Parents[parent]; exists {
			continue
		}
		g.Parents[parent] = struct{}{}
		return parent
	}
}
