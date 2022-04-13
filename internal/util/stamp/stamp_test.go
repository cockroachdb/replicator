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
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

// intStamp is its own Stamp.
type intStamp int

var _ Coalescing = intStamp(0)
var _ Stamp = intStamp(0)
var _ Stamped = intStamp(0)

// Create a deduplication effect of identical values.
func (s intStamp) Coalesce(tail Stamped) Stamped {
	o := tail.(intStamp)
	if o == s {
		return nil
	}
	return tail
}

func (s intStamp) String() string {
	return strconv.FormatInt(int64(s), 10)
}

func (s intStamp) Less(other Stamp) bool {
	if ptr, ok := other.(*intStamp); ok {
		return s < *ptr
	}
	return s < other.(intStamp)
}

func (s intStamp) Stamp() Stamp {
	return s
}

func TestCompareStamp(t *testing.T) {
	one := intStamp(1)
	otherOne := intStamp(1)
	two := intStamp(2)

	tcs := []struct {
		a, b     Stamp
		expected int
	}{
		{nil, nil, 0},
		{nil, one, -1},
		{one, nil, 1},

		{Stamp(nil), Stamp(nil), 0},
		{Stamp(nil), one, -1},
		{one, Stamp(nil), 1},

		{one, two, -1},
		{two, one, 1},
		{one, one, 0},
		{two, two, 0},

		{&one, &otherOne, 0},
	}

	for idx, tc := range tcs {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			a := assert.New(t)
			a.Equal(tc.expected, Compare(tc.a, tc.b))
		})
	}
}
