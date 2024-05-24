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

package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/replicator/internal/source/objstore/providers/tests"
	minio "github.com/minio/minio-go/v7"
)

var (
	Errors = map[tests.Errors]error{
		tests.NoSuchBucket: errors.New("bucket not found"),
		tests.NoSuchKey:    errors.New("key not found"),
	}
)

// mockS3 is in memory S3 bucket.
type mockS3 struct {
	bucketName string
	files      sync.Map
}

var _ s3Access = &mockS3{}
var _ tests.Writer = &mockS3{}

// GetObject implements s3Access.
func (m *mockS3) GetObject(
	ctx context.Context, bucketName string, objectName string, opts minio.GetObjectOptions,
) (io.ReadCloser, error) {
	if bucketName != m.bucketName {
		return nil, Errors[tests.NoSuchBucket]
	}
	file, ok := m.files.Load(objectName)
	if !ok {
		return nil, Errors[tests.NoSuchKey]
	}
	return io.NopCloser(bytes.NewReader(file.([]byte))), nil
}

// ListObjects implements s3Access.
func (m *mockS3) ListObjects(
	ctx context.Context, bucketName string, opts minio.ListObjectsOptions,
) <-chan minio.ObjectInfo {
	ch := make(chan minio.ObjectInfo)
	go func() {
		defer close(ch)

		if bucketName != m.bucketName {
			ch <- minio.ObjectInfo{
				Err: Errors[tests.NoSuchBucket],
			}
			return
		}
		files := make([]string, 0, 10)
		m.files.Range(func(key any, value any) bool {
			files = append(files, key.(string))
			return true
		})
		sort.Strings(files)
		count := 0
		for _, f := range files {
			if opts.StartAfter != "" && strings.Compare(f, opts.StartAfter) <= 0 {
				continue
			}
			ch <- minio.ObjectInfo{
				Key: f,
			}
			count++
			if opts.MaxKeys > 0 && count >= opts.MaxKeys {
				return
			}
		}
	}()
	return ch
}

// Store implements validate.Writer.
func (m *mockS3) Store(ctx context.Context, name string, buf []byte) error {
	m.files.Store(name, buf)
	return nil
}

func TestOpen(t *testing.T) {
	suite().Open(t)
}

func TestOverwrite(t *testing.T) {
	suite().Overwrite(t)
}

func TestWalk(t *testing.T) {
	suite().Walk(t)
}

func suite() *tests.Suite {
	mockS3 := &mockS3{
		bucketName: "test",
	}
	return &tests.Suite{
		Errors: Errors,
		Reader: &s3Bucket{
			client: mockS3,
			bucket: "test",
		},
		Writer: mockS3,
	}
}
