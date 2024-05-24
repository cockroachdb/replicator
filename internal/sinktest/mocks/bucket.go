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

package mocks

import (
	"io"
	"math/rand"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/source/objstore/bucket"
)

// chaosBucket wraps a bucket.Bucket and injects transient errors.
type chaosBucket struct {
	delegate           bucket.Bucket
	probTransientError float32
}

var _ bucket.Bucket = &chaosBucket{}

// NewChaosBucket creates a new shim around a bucket that throws
// intermittent transient errors.
func NewChaosBucket(delegate bucket.Bucket, prob float32) bucket.Bucket {
	return &chaosBucket{
		delegate:           delegate,
		probTransientError: prob,
	}
}

// Open implements bucket.Bucket.
func (c *chaosBucket) Open(ctx *stopper.Context, path string) (io.ReadCloser, error) {
	if rand.Float32() <= c.probTransientError {
		return nil, bucket.ErrTransient
	}
	return c.delegate.Open(ctx, path)
}

// Walk implements bucket.Bucket.
func (c *chaosBucket) Walk(
	ctx *stopper.Context,
	prefix string,
	options *bucket.WalkOptions,
	f func(*stopper.Context, string) error,
) error {
	if rand.Float32() <= c.probTransientError {
		return bucket.ErrTransient
	}
	return c.delegate.Walk(ctx, prefix, options, f)
}
