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

package local

import (
	"context"
	"path/filepath"
	"testing"
	"testing/fstest"
	"time"

	"github.com/cockroachdb/replicator/internal/source/objstore/providers/storetest"
	"github.com/stretchr/testify/require"
)

// mockFS is in memory filesystem.
type mockFS struct {
	root      fstest.MapFS
	directory string
}

var _ storetest.Writer = &mockFS{}

// Store implements validate.Writer.
func (m *mockFS) Store(ctx context.Context, name string, buf []byte) error {
	name = filepath.Clean(filepath.Join(m.directory, name))
	m.root[name] = &fstest.MapFile{
		Data:    []byte(buf),
		Mode:    0777,
		ModTime: time.Now(),
	}
	return nil
}

// TestOpen verifies that we can get an object content from the store.
func TestOpen(t *testing.T) {
	suite(t).Open(t)
}

// TestOpen verifies that we can get an object content from the store.
func TestOverwrite(t *testing.T) {
	suite(t).Overwrite(t)
}

// TestWalk verifies that we can list objects in the store.
func TestWalk(t *testing.T) {
	suite(t).Walk(t)
}

// WalkWithSkipAll validates bucket.Bucket.Walk with ErrSkipAll
func TestWalkWithSkipAll(t *testing.T) {
	suite(t).WalkWithSkipAll(t)
}

// suite builds a validator for a in memory filesystem.
func suite(t *testing.T) *storetest.Suite {
	rootFS := make(fstest.MapFS)
	mockFS := &mockFS{
		directory: "tmp",
		root:      rootFS,
	}
	bucketFS, err := rootFS.Sub("tmp")
	require.NoError(t, err)
	return &storetest.Suite{
		Reader: &localBucket{
			filesystem: bucketFS,
		},
		Writer: mockFS,
	}
}
