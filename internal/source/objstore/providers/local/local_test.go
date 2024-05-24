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
	"errors"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/replicator/internal/source/objstore/providers/validate"
	"github.com/psanford/memfs"
	"github.com/stretchr/testify/require"
)

var (
	Errors = map[validate.Errors]error{
		validate.NoSuchBucket: errors.New("does not exist"),
		validate.NoSuchKey:    errors.New("does not exist"),
	}
)

// mockFS is in memory filesystem.
type mockFS struct {
	root      *memfs.FS
	directory string
}

var _ validate.Writer = &mockFS{}

// Store implements validate.Writer.
func (m *mockFS) Store(ctx context.Context, name string, buf []byte) error {
	name = filepath.Clean(filepath.Join(m.directory, name))
	dir := filepath.Dir(name)
	// In case the directory doens't exist, we will create it.
	err := m.root.MkdirAll(dir, 0777)
	if err != nil {
		return err
	}
	return m.root.WriteFile(name, buf, 0777)
}

// TestGet verifies that we can get an object content from the store.
func TestGet(t *testing.T) {
	newValidator(t).Get(t)
}

// TestIter verifies that we can list objects in the store.
func TestIter(t *testing.T) {
	newValidator(t).Iter(t)
}

// newValidator builds a validator for a in memory filesystem.
func newValidator(t *testing.T) *validate.Validator {
	rootFS := memfs.New()
	err := rootFS.MkdirAll("tmp", 0777)
	require.NoError(t, err)
	bucketFS, err := rootFS.Sub("tmp")
	require.NoError(t, err)
	mockFS := &mockFS{
		directory: "tmp",
		root:      rootFS,
	}
	return &validate.Validator{
		Errors: Errors,
		Reader: &localBucket{
			filesystem: bucketFS,
		},
		Writer: mockFS,
	}
}
