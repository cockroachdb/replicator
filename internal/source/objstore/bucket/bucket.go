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
	"context"
	"io"
)

// IterOptions are the configuration options used by the iterators.
type IterOptions struct {
	Max        int    // Maximum number of entries to return.
	Recursive  bool   // Enable recursive descend.
	StartAfter string // Iterators will returns entries lexically after this.
}

// Reader provides read access to an object storage bucket.
type Reader interface {
	// Iter calls f for each entry in the given directory. The argument
	// to f is the full object name including the prefix of the
	// inspected directory. Entries are passed to function in sorted
	// order.
	Iter(ctx context.Context, dir string, f func(string) error, options IterOptions) error

	// Get returns a reader for the given object name.
	Get(ctx context.Context, name string) (io.ReadCloser, error)
}
