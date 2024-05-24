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

// Package validate defines the tests that the providers must pass.
package validate

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/replicator/internal/source/objstore/bucket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Errors defines the errors we expect from the tests.
type Errors int

// The error messages that we expect in the tests.
const (
	NoSuchBucket Errors = iota
	NoSuchKey
)

// Writer allows the validator to add objects to the bucket.
type Writer interface {
	// Store writes the content to the bucket.
	Store(ctx context.Context, name string, buf []byte) error
}

// Validator verifies that the providers for bucket.Reader can
// read and list objects from a bucket.
// TODO (silvano): expand the test cases, add integration tests.
type Validator struct {
	Errors map[Errors]error
	Reader bucket.Reader
	Writer Writer
}

// Get validates bucket.Reader.Get
func (v *Validator) Get(t *testing.T) {
	r := require.New(t)
	tests := []struct {
		name    string
		file    string
		want    []byte
		wantErr string
	}{
		{"found", "test.txt", []byte("test"), ""},
		{"notfound", "nothere.txt", nil, v.Errors[NoSuchKey].Error()},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r.NoError(v.Writer.Store(ctx, "test.txt", []byte("test")))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := assert.New(t)
			r := require.New(t)
			got, err := v.Reader.Get(ctx, tt.file)
			if tt.wantErr != "" {
				a.ErrorContains(err, tt.wantErr)
				return
			}
			r.NoError(err)
			res := make([]byte, len(tt.want))
			n, err := got.Read(res)
			a.NoError(err)
			a.Equal(len(tt.want), n)
			a.Equal(tt.want, res)
		})
	}
}

// Iter validates bucket.Reader.Iter
func (v *Validator) Iter(t *testing.T) {
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
	r.NoError(v.Writer.Store(ctx, "000/000.txt", []byte("test")))
	r.NoError(v.Writer.Store(ctx, "000/001.txt", []byte("test")))
	r.NoError(v.Writer.Store(ctx, "000/002.txt", []byte("test")))
	r.NoError(v.Writer.Store(ctx, "000/003.txt", []byte("test")))
	r.NoError(v.Writer.Store(ctx, "001/000.txt", []byte("test")))
	r.NoError(v.Writer.Store(ctx, "001/001.txt", []byte("test")))
	r.NoError(v.Writer.Store(ctx, "001/002.txt", []byte("test")))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := assert.New(t)
			r := require.New(t)
			res := make([]string, 0)
			options := bucket.IterOptions{
				StartAfter: tt.prefix,
				Max:        tt.max,
				Recursive:  true,
			}
			err := v.Reader.Iter(ctx, "", func(s string) error {
				res = append(res, s)
				return nil
			}, options)
			r.NoError(err)
			a.Equal(tt.want, res)
		})
	}
}
