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
	"context"
	"io"

	"github.com/minio/minio-go/v7"
)

// client wraps a *minio.Client so we can modify the GetObject function
// to return a io.ReadCloser rather than a *minio.Object. This
// simplifies testing.
type client struct {
	ref *minio.Client
}

var _ s3Access = &client{}

// GetObject implements s3Access.
func (c *client) GetObject(
	ctx context.Context, bucketName string, objectName string, opts minio.GetObjectOptions,
) (io.ReadCloser, error) {
	// GetObject returns  a *minio.Object, but we only care about the io.ReadCloser
	// interface that the returned object implements.
	return c.ref.GetObject(ctx, bucketName, objectName, opts)
}

// ListObjects implements s3Access.
func (c *client) ListObjects(
	ctx context.Context, bucketName string, opts minio.ListObjectsOptions,
) <-chan minio.ObjectInfo {
	return c.ref.ListObjects(ctx, bucketName, opts)
}
