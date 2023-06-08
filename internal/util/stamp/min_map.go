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

import "container/heap"

// A MinMap provides a mapping of keys to Stamp values, while also
// tracking the minimum value.
//
// A MinMap is not internally synchronized. A MinMap should not be
// copied once created.
type MinMap struct {
	m map[any]*minMapElt
	h minMapHeap

	// Prevent Lock() methods from escaping into API.
	_ struct {
		noCopy
	}
}

// NewMinMap constructs an empty MinMap.
func NewMinMap() *MinMap {
	return &MinMap{
		m: make(map[any]*minMapElt),
	}
}

// Delete removes the mapping for the specified key, if one is present.
func (m *MinMap) Delete(key any) {
	elt, ok := m.m[key]
	if !ok {
		return
	}
	delete(m.m, key)
	heap.Remove(&m.h, elt.Index)
}

// Get returns the previously set Stamp for the key, or nil if no
// mapping is present.
func (m *MinMap) Get(key any) (Stamp, bool) {
	if found, ok := m.m[key]; ok {
		return found.Stamp, true
	}
	return nil, false
}

// Len returns the number of elements within the map.
func (m *MinMap) Len() int {
	return len(m.h)
}

// Min returns the minimum Stamp in the map, or nil if the map is empty.
func (m *MinMap) Min() Stamp {
	if len(m.h) > 0 {
		return m.h[0].Stamp
	}
	return nil
}

// Put adds or updates an entry in the map. This method returns the
// minimum Stamp within the map and a boolean flag to indicate if the
// call to Put changed the minimum value.
func (m *MinMap) Put(key any, stamp Stamp) (minStamp Stamp, minChanged bool) {
	startMin := m.Min()
	if elt, ok := m.m[key]; ok {
		// If there's already an entry in the map, update the Stamp
		// and then fix the heap ordering.
		elt.Stamp = stamp
		heap.Fix(&m.h, elt.Index)
	} else {
		// Otherwise, we just add the new element to the lookup map
		// and to the heap.
		elt = &minMapElt{Stamp: stamp}
		heap.Push(&m.h, elt)
		m.m[key] = elt
	}
	endMin := m.h[0].Stamp
	return endMin, Compare(startMin, endMin) != 0
}

// A minMapElt is an entry in a minMapHeap which tracks its current
// index. This allows us to reduce the cost of restoring the heap
// invariants when the Stamp is updated.
type minMapElt struct {
	Index int
	Stamp Stamp
}

// A min-heap of elements.
type minMapHeap []*minMapElt

var _ heap.Interface = (*minMapHeap)(nil)

// Len implements heap.Interface.
func (h minMapHeap) Len() int {
	return len(h)
}

// Less implements heap.Interface.
func (h minMapHeap) Less(i, j int) bool {
	return Compare(h[i].Stamp, h[j].Stamp) < 0
}

// Pop implements heap.Interface.
func (h *minMapHeap) Pop() any {
	idx := len(*h) - 1
	elt := (*h)[idx]
	*h = (*h)[:idx]
	return elt
}

// Push implements heap.Interface.
func (h *minMapHeap) Push(x any) {
	elt := x.(*minMapElt)
	elt.Index = len(*h)
	*h = append(*h, elt)
}

// Swap implements heap.Interface.
func (h minMapHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].Index, h[j].Index = i, j
}
