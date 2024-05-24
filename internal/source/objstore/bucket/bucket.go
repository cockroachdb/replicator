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

// Package bucket defines the interface that the providers must implement
// to access cloud storage.
package bucket

import (
	"io"

	"github.com/cockroachdb/field-eng-powertools/stopper"
)

// WalkOptions are the configuration options used by the iterators.
// While buckets are generally not hierarchial, they support the concept
// of folders, grouping objects identified by a common prefix.
// Furthermore, since this package is currently used to process events
// produced by CockroachDB changefeeds, we assume they are store using
// one of the following formats:
// - daily folders: /[date]/[timestamp]-[uniquer]-[topic]-[schema-id]
// - hourly folders: /[date]/[hour]/[timestamp]-[uniquer]-[topic]-[schema-id]
// - flat: /[timestamp]-[uniquer]-[topic]-[schema-id]
// In context, the use the terms recursive to indicate the ability to descent
// in nested folders. A folder is separated by "/", similar to Unix directories.
type WalkOptions struct {
	Limit      int    // Maximum number of entries to return. If zero or negative, no limit.
	Recursive  bool   // Enable recursive descent.
	StartAfter string // Iterators will returns entries lexically after this.
}

// Reader provides read access to an object storage bucket.
type Reader interface {
	// Walk calls f for each entry in the given prefix. The argument
	// to f is the full object name including the prefix of the
	// inspected directory. Entries are passed to function in sorted
	// order.
	Walk(ctx *stopper.Context, prefix string, options *WalkOptions, f func(*stopper.Context, string) error) error

	// Open returns a reader for the object at the named path.
	Open(ctx *stopper.Context, path string) (io.ReadCloser, error)
}
