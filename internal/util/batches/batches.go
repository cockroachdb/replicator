// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package batches contains support code for working with and testing
// batches of data.
package batches

import (
	"flag"
	"sync"

	"github.com/cockroachdb/cdc-sink/internal/sinktypes"
)

const defaultSize = 1000

var batchSize = flag.Int("batchSize", defaultSize, "default size for batched operations")

// Batch is a helper to perform some operation over a large number
// of values in a batch-oriented fashion. The indexes provided to
// the callback function are a half-open range [begin , end).
func Batch(count int, fn func(begin, end int) error) error {
	consume := Size()
	idx := 0
	for {
		if consume > count {
			consume = count
		}
		if err := fn(idx, idx+consume); err != nil {
			return err
		}
		if consume == count {
			return nil
		}
		idx += consume
		count -= consume
	}
}

// Size returns the default size for batch operations. Testing code
// should generally use a multiple of this value to ensure that
// batching has been correctly implemented.
func Size() int {
	x := batchSize
	if x == nil {
		return defaultSize
	}
	return *x
}

// The Release function must be called to return the underlying array
// back to the pool.
type Release func()

var intPool = &sync.Pool{New: func() interface{} {
	x := make([]int, 0, Size())
	return &x
}}

// Int returns a slice of Size() capacity.
func Int() ([]int, Release) {
	ret := intPool.Get().(*[]int)
	return *ret, func() { intPool.Put(ret) }
}

var int64Pool = &sync.Pool{New: func() interface{} {
	x := make([]int64, 0, Size())
	return &x
}}

// Int64 returns a slice of Size() capacity.
func Int64() ([]int64, Release) {
	ret := int64Pool.Get().(*[]int64)
	return *ret, func() { int64Pool.Put(ret) }
}

var mutationPool = &sync.Pool{New: func() interface{} {
	x := make([]sinktypes.Mutation, 0, Size())
	return &x
}}

// Mutation returns a slice of Size() capacity.
func Mutation() ([]sinktypes.Mutation, Release) {
	ret := mutationPool.Get().(*[]sinktypes.Mutation)
	return *ret, func() { mutationPool.Put(ret) }
}

var stringPool = &sync.Pool{New: func() interface{} {
	x := make([]string, 0, Size())
	return &x
}}

// String returns a slice of Size() capacity.
func String() ([]string, Release) {
	ret := stringPool.Get().(*[]string)
	return *ret, func() { stringPool.Put(ret) }
}
