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

	"github.com/cockroachdb/cdc-sink/internal/sequencer"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/all"
	"github.com/cockroachdb/cdc-sink/internal/sinktest/base"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/notify"
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

	ctr int
}

// NewGenerator constructs a Generator that will create messages
// for parent and child tables.
func NewGenerator(
	ctx context.Context, fixture *all.Fixture,
) (*Generator, *types.TableGroup, error) {
	parentInfo, err := fixture.CreateTargetTable(ctx, "CREATE TABLE %s (parent INT PRIMARY KEY)")
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
			Child:    childInfo,
			Children: make(map[int]struct{}),
			Fixture:  fixture,
			Parent:   parentInfo,
			Parents:  make(map[int]struct{}),
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
	r := assert.New(t)

	// Verify all mutations have been unstaged.
	rng := g.Range()
	staged, err := g.Fixture.PeekStaged(ctx, g.Parent.Name(), rng.Min(), rng.Max())
	r.NoError(err)
	r.Emptyf(staged, "staging table %s not empty", g.Parent)
	staged, err = g.Fixture.PeekStaged(ctx, g.Child.Name(), rng.Min(), rng.Max())
	r.NoError(err)
	r.Emptyf(staged, "staging table %s not empty", g.Child)

	// Verify target row counts against generated data.
	parentCount, err := g.Parent.RowCount(ctx)
	r.NoError(err)
	r.Equalf(len(g.Parents), parentCount, "parent %s", g.Parent.Name())
	childCount, err := g.Child.RowCount(ctx)
	r.NoError(err)
	r.Equal(len(g.Children), childCount, "child %s", g.Child.Name())
}

// GenerateInto will add mutations to the batch at the requested time.
func (g *Generator) GenerateInto(batch *types.MultiBatch, time hlc.Time) {
	switch g.ctr % 5 {
	case 0: // Insert a parent row
		parent := g.pickNewParent()
		_ = batch.Accumulate(g.Parent.Name(), types.Mutation{
			Data: json.RawMessage(fmt.Sprintf(`{ "parent": %d }`, parent)),
			Key:  json.RawMessage(fmt.Sprintf(`[ %d ]`, parent)),
			Time: time,
		})

	case 1: // Update a parent row
		parent := g.pickExistingParent()
		_ = batch.Accumulate(g.Parent.Name(), types.Mutation{
			Data: json.RawMessage(fmt.Sprintf(`{ "parent": %d }`, parent)),
			Key:  json.RawMessage(fmt.Sprintf(`[ %d ]`, parent)),
			Time: time,
		})

	case 2: // Insert a child row referencing an existing parent
		parent := g.pickExistingParent()
		child := g.pickNewChild()
		_ = batch.Accumulate(g.Child.Name(), types.Mutation{
			Data: json.RawMessage(fmt.Sprintf(`{ "child": %d, "parent": %d }`, child, parent)),
			Key:  json.RawMessage(fmt.Sprintf(`[ %d ]`, child)),
			Time: time,
		})

	case 3: // Insert a new child row referencing a new parent
		parent := g.pickNewParent()
		_ = batch.Accumulate(g.Parent.Name(), types.Mutation{
			Data: json.RawMessage(fmt.Sprintf(`{ "parent": %d }`, parent)),
			Key:  json.RawMessage(fmt.Sprintf(`[ %d ]`, parent)),
			Time: time,
		})

		child := g.pickNewChild()
		_ = batch.Accumulate(g.Child.Name(), types.Mutation{
			Data: json.RawMessage(fmt.Sprintf(`{ "child": %d, "parent": %d }`, child, parent)),
			Key:  json.RawMessage(fmt.Sprintf(`[ %d ]`, child)),
			Time: time,
		})

	case 4: // Re-parent an existing child
		parent := g.pickExistingParent()
		child := g.pickExistingChild()
		_ = batch.Accumulate(g.Child.Name(), types.Mutation{
			Data: json.RawMessage(fmt.Sprintf(`{ "child": %d, "parent": %d }`, child, parent)),
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
		min := sequencer.CommonMin(stat)
		if hlc.Compare(min, g.MaxTime) >= 0 {
			log.Debugf("caught up to %s", min)
			return nil
		}
		log.Debugf("waiting for catch-up: %s vs %s", min, g.MaxTime)
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
