// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stamp

import (
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

	var lastMin Stamp
	for _, elt := range elts {
		lastMin, _ = m.Put(int(elt), elt)
	}

	a.Equal(intStamp(0), lastMin)
	a.Equal(lastMin, m.Min())
	a.Equal(count, m.Len())

	// Get all values.
	for idx := range elts {
		a.Equal(intStamp(idx), m.Get(idx))
	}
	a.Nil(m.Get("not found"))

	// Replace all values.
	for idx := range elts {
		lastMin, _ = m.Put(idx, intStamp(count+idx))
	}
	a.Equal(intStamp(count), lastMin)

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
	lastMin, changed := m.Put("nil", nil)
	a.Nil(lastMin)
	a.False(changed) // The previous result for Min() was already nil.

	lastMin, changed = m.Put("nil2", nil)
	a.Nil(lastMin)
	a.False(changed)
}
