// Copyright 2023 The Cockroach Authors
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

package stamp

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMinMap(t *testing.T) {
	a := assert.New(t)

	// Insert some elements in random order.
	const count = 1024
	elts := make([]intStamp, count)
	for idx := range elts {
		elts[idx] = intStamp(idx)
	}
	rand.Shuffle(count, func(i, j int) { elts[i], elts[j] = elts[j], elts[i] })

	m := NewMinMap()
	a.Equal(0, m.Len())
	a.Nil(m.Min())
	found, ok := m.Get("foo")
	a.Nil(found)
	a.False(ok)

	var currentMin Stamp                    // The current min value in the map.
	var expectedMin = intStamp(math.MaxInt) // Independent check.

	for idx, elt := range elts {
		if elt < expectedMin {
			expectedMin = elt
		}
		currentMin, _ = m.Put(int(elt), elt)
		a.Equal(expectedMin, currentMin)
		a.Equal(idx+1, m.Len())
		for i := 0; i < idx; i++ {
			found, ok := m.Get(int(elt))
			a.Equal(elt, found)
			a.True(ok)
		}
	}

	a.Equal(intStamp(0), currentMin)
	a.Equal(currentMin, m.Min())
	a.Equal(count, m.Len())

	// Get all values.
	for idx := range elts {
		found, ok := m.Get(idx)
		a.Equal(intStamp(idx), found)
		a.True(ok)
	}
	a.Nil(m.Get("not found"))

	// Replace all values.
	for idx := range elts {
		currentMin, _ = m.Put(idx, intStamp(count+idx))
	}
	a.Equal(intStamp(count), currentMin)

	// Delete all values.
	for _, elt := range elts {
		m.Delete(int(elt))
	}

	a.Equal(0, m.Len())
	a.Nil(m.Get(0))
	a.Nil(m.Min())

	// Verify redundant delete is ok.
	m.Delete(0)

	// Verify that adding a nil value is ok.
	currentMin, changed := m.Put("nil", nil)
	a.Nil(currentMin)
	a.False(changed) // The previous result for Min() was already nil.

	currentMin, changed = m.Put("nil2", nil)
	a.Nil(currentMin)
	a.False(changed)
}
