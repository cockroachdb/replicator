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

package mutations

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/stretchr/testify/assert"
)

func TestEqual(t *testing.T) {
	a := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Empty case.
	a.True(Equal(types.Mutation{}, types.Mutation{}))

	// Also test the generator while we're at it.
	source := Generator(ctx, 1024, 0.1)
	for i := 0; i < 1024; i++ {
		mut := <-source
		if !a.True(Equal(mut, mut)) {
			data, err := json.Marshal(&mut)
			if a.NoError(err) {
				a.Failf("mutation was not equal to itself", "%s", string(data))
			}
		}
		a.False(Equal(mut, types.Mutation{}))
	}
}
