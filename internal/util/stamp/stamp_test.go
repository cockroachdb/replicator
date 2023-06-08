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

func (s intStamp) MarshalText() (text []byte, err error) {
	return []byte(strconv.FormatInt(int64(s), 10)), nil
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
