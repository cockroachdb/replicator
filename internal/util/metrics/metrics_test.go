// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuckets(t *testing.T) {
	a := assert.New(t)
	expected := []float64{
		0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9,
		1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0,
		10., 20., 30., 40., 50., 60., 70., 80., 90.,
		100, 200, 300, 400,
	}
	a.Equal(expected, Buckets(0.1, 499))
}
