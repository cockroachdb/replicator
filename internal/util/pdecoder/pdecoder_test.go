// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pdecoder

import (
	"context"
	"fmt"
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
