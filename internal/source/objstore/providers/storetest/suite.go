// Copyright 2024 The Cockroach Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://wwv.Writer.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

// Package storetest defines the tests that the providers must pass.
package storetest

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/source/objstore/bucket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Writer allows the test suite to add objects to the bucket.
type Writer interface {
	// Store writes the content to the bucket.
	Store(ctx context.Context, name string, buf []byte) error
}

// Suite verifies that the providers for bucket.Reader can
// read and list objects from a bucket.
// TODO (silvano): expand the test cases, add integration tests.
type Suite struct {
	Reader bucket.Reader // The interface we are testing.
	Writer Writer        // The interface used to load objects for testing.
}

// Open validates bucket.Reader.Open
func (v *Suite) Open(t *testing.T) {
	r := require.New(t)
	tests := []struct {
		name    string
		file    string
		want    string
		wantErr error
	}{
		{"found", "test.txt", "test", nil},
		{"notfound", "nothere.txt", "", bucket.ErrNoSuchKey},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stop := stopper.WithContext(ctx)
	r.NoError(v.Writer.Store(ctx, "test.txt", []byte("test")))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := assert.New(t)
			r := require.New(t)
			got, err := v.read(stop, tt.file)
			if tt.wantErr != nil {
				a.ErrorIs(err, tt.wantErr)
				return
			}
			r.NoError(err)
			a.Equal(tt.want, got)
		})
	}
}

// Overwrite validates bucket.Reader.Open reads the latest version of a file.
func (v *Suite) Overwrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stop := stopper.WithContext(ctx)
	r := require.New(t)
	a := assert.New(t)
	path := "test.txt"
	for _, s := range []string{"v0", "v1", "v2", "v3"} {
		r.NoError(v.Writer.Store(ctx, path, []byte(s)))
		got, err := v.read(stop, path)
		r.NoError(err)
		a.Equal(s, got)
	}
}

// Walk validates bucket.Reader.Walk
func (v *Suite) Walk(t *testing.T) {
	r := require.New(t)
	tests := []struct {
		name   string
		prefix string
		max    int
		want   []string
	}{
		{"no prefix", "", 3, []string{
			"000/000.txt",
			"000/001.txt",
			"000/002.txt",
		}},
		{"prefix", "000/002.txt", 3, []string{
			"000/003.txt",
			"001/000.txt",
			"001/001.txt",
		}},
		{"prefix", "001", 2, []string{
			"001/000.txt",
			"001/001.txt",
		}},
		{"all", "", 0, []string{
			"000/000.txt",
			"000/001.txt",
			"000/002.txt",
			"000/003.txt",
			"001/000.txt",
			"001/001.txt",
			"001/002.txt",
		}},
		{"none", "002", 4, []string{}},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stop := stopper.WithContext(ctx)
	r.NoError(v.createDummy(ctx, "000/000.txt"))
	r.NoError(v.createDummy(ctx, "000/001.txt"))
	r.NoError(v.createDummy(ctx, "000/002.txt"))
	r.NoError(v.createDummy(ctx, "000/003.txt"))

	r.NoError(v.createDummy(ctx, "001/000.txt"))
	r.NoError(v.createDummy(ctx, "001/001.txt"))
	r.NoError(v.createDummy(ctx, "001/002.txt"))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := assert.New(t)
			r := require.New(t)
			res := make([]string, 0)
			options := &bucket.WalkOptions{
				StartAfter: tt.prefix,
				Limit:      tt.max,
				Recursive:  true,
			}
			err := v.Reader.Walk(stop, "", options, func(_ *stopper.Context, s string) error {
				res = append(res, s)
				return nil
			})
			r.NoError(err)
			a.Equal(tt.want, res)
		})
	}
}

// WalkWithSkipAll validates bucket.Reader.Walk with ErrSkipAll
func (v *Suite) WalkWithSkipAll(t *testing.T) {
	r := require.New(t)
	tests := []struct {
		name  string
		until string
		max   int
		want  []string
	}{
		{"stop at 4", "000/003.txt", 0, []string{
			"000/000.txt",
			"000/001.txt",
			"000/002.txt",
			"000/003.txt",
		}},
		{"stop at 6", "001/001.txt", 0, []string{
			"000/000.txt",
			"000/001.txt",
			"000/002.txt",
			"000/003.txt",
			"001/000.txt",
			"001/001.txt",
		}},
		{"all", "001/003.txt", 0, []string{
			"000/000.txt",
			"000/001.txt",
			"000/002.txt",
			"000/003.txt",
			"001/000.txt",
			"001/001.txt",
			"001/002.txt",
		}},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stop := stopper.WithContext(ctx)
	r.NoError(v.createDummy(ctx, "000/000.txt"))
	r.NoError(v.createDummy(ctx, "000/001.txt"))
	r.NoError(v.createDummy(ctx, "000/002.txt"))
	r.NoError(v.createDummy(ctx, "000/003.txt"))

	r.NoError(v.createDummy(ctx, "001/000.txt"))
	r.NoError(v.createDummy(ctx, "001/001.txt"))
	r.NoError(v.createDummy(ctx, "001/002.txt"))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := assert.New(t)
			r := require.New(t)
			res := make([]string, 0)
			options := &bucket.WalkOptions{
				StartAfter: "",
				Limit:      tt.max,
				Recursive:  true,
			}
			err := v.Reader.Walk(stop, "", options, func(_ *stopper.Context, s string) error {
				res = append(res, s)
				if s == tt.until {
					return bucket.ErrSkipAll
				}
				return nil
			})
			r.NoError(err)
			a.Equal(tt.want, res)
		})
	}
}

// createDummy creates a new object at the named path. The content of the object
// is the path itself.
func (v *Suite) createDummy(ctx context.Context, path string) error {
	return v.Writer.Store(ctx, path, []byte(path))
}

// read returns a string with the content of the object at named path.
func (v *Suite) read(ctx *stopper.Context, path string) (string, error) {
	r, err := v.Reader.Open(ctx, path)
	if err != nil {
		return "", err
	}
	buf := new(strings.Builder)
	_, err = io.Copy(buf, r)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}
