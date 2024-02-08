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
	"encoding/json"
	"fmt"
	"math/rand"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
)

// GenerateBatch will generate a batch of data at a specific time. It
// assumes that there is a parent table and a child table. The parents
// map will be updated whenever a new parent entry is required.
func GenerateBatch(
	ctr *int, time hlc.Time, parents, children map[int]struct{}, parentTbl, childTbl ident.Table,
) *types.MultiBatch {
	pickExistingChild := func() int {
		// Rely on random iteration order.
		for child := range children {
			return child
		}
		panic("no children")
	}
	pickExistingParent := func() int {
		// Rely on random iteration order.
		for parent := range parents {
			return parent
		}
		panic("no parents")
	}
	pickNewChild := func() int {
		for {
			child := int(rand.Int31())
			if _, exists := children[child]; exists {
				continue
			}
			children[child] = struct{}{}
			return child
		}
	}
	pickNewParent := func() int {
		for {
			parent := int(rand.Int31())
			if _, exists := parents[parent]; exists {
				continue
			}
			parents[parent] = struct{}{}
			return parent
		}
	}

	batch := &types.MultiBatch{}
	switch *ctr % 5 {
	case 0: // Insert a parent row
		parent := pickNewParent()
		_ = batch.Accumulate(parentTbl, types.Mutation{
			Data: json.RawMessage(fmt.Sprintf(`{ "parent": %d }`, parent)),
			Key:  json.RawMessage(fmt.Sprintf(`[ %d ]`, parent)),
			Time: time,
		})

	case 1: // Update a parent row
		parent := pickExistingParent()
		_ = batch.Accumulate(parentTbl, types.Mutation{
			Data: json.RawMessage(fmt.Sprintf(`{ "parent": %d }`, parent)),
			Key:  json.RawMessage(fmt.Sprintf(`[ %d ]`, parent)),
			Time: time,
		})

	case 2: // Insert a child row referencing an existing parent
		parent := pickExistingParent()
		child := pickNewChild()
		_ = batch.Accumulate(childTbl, types.Mutation{
			Data: json.RawMessage(fmt.Sprintf(`{ "child": %d, "parent": %d }`, child, parent)),
			Key:  json.RawMessage(fmt.Sprintf(`[ %d ]`, child)),
			Time: time,
		})

	case 3: // Insert a new child row referencing a new parent
		parent := pickNewParent()
		_ = batch.Accumulate(parentTbl, types.Mutation{
			Data: json.RawMessage(fmt.Sprintf(`{ "parent": %d }`, parent)),
			Key:  json.RawMessage(fmt.Sprintf(`[ %d ]`, parent)),
			Time: time,
		})

		child := pickNewChild()
		_ = batch.Accumulate(childTbl, types.Mutation{
			Data: json.RawMessage(fmt.Sprintf(`{ "child": %d, "parent": %d }`, child, parent)),
			Key:  json.RawMessage(fmt.Sprintf(`[ %d ]`, child)),
			Time: time,
		})

	case 4: // Re-parent an existing child
		parent := pickExistingParent()
		child := pickExistingChild()
		_ = batch.Accumulate(childTbl, types.Mutation{
			Data: json.RawMessage(fmt.Sprintf(`{ "child": %d, "parent": %d }`, child, parent)),
			Key:  json.RawMessage(fmt.Sprintf(`[ %d ]`, child)),
			Time: time,
		})

	default:
		panic("check your modulus")
	}

	*ctr++
	return batch
}
