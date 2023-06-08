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

package pjson

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDecode(t *testing.T) {
	const count = 1000
	a := assert.New(t)

	src := make([][]byte, count)
	for i := range src {
		src[i] = []byte(fmt.Sprintf("%d", i))
	}

	dest := make([]int, count)
	a.NoError(Decode(context.Background(), dest, func(i int) []byte {
		return src[i]
	}))
	for i := range dest {
		a.Equal(i, dest[i])
	}
}

func TestEncode(t *testing.T) {
	const count = 1000
	a := assert.New(t)

	dest := make([][]byte, count)
	a.NoError(Encode(context.Background(), dest, func(i int) int {
		return i
	}))

	for i := range dest {
		a.Equal([]byte(strconv.Itoa(i)), dest[i])
	}
}
