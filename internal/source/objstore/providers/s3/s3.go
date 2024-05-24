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

// Package s3 provides access to AWS S3 bucket
package s3

import (
	"context"
	"io"
	"strings"

	"github.com/cockroachdb/replicator/internal/source/objstore/bucket"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	log "github.com/sirupsen/logrus"
)

const (
	// DirDelim is the directory delimiter used in S3.
	DirDelim = "/"
)

// Config has the parameters used to connect to S3.
type Config struct {
	AccessKey string // AWS Access Key
	Bucket    string // The name of the bucket.
	Endpoint  string // Alternative server to use, for other S3 providers.
	Insecure  bool   // For testing against self hosted S3 providers.
	SecretKey string // Secret associated to the Access Key
}

// s3Access defines the functions we are using to interact with the minio SDK.
// Mainly used for testing to implement a mock component.
type s3Access interface {
	// GetObject returns the content of the named object.
	GetObject(ctx context.Context, bucketName string, objectName string, opts minio.GetObjectOptions) (io.ReadCloser, error)
	// ListObjects scans the entries in the bucket.
	ListObjects(ctx context.Context, bucketName string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo
}

// New returns a bucket reader backed by a S3 provider.
func New(config *Config) (bucket.Reader, error) {
	minioClient, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.AccessKey, config.SecretKey, ""),
		Secure: !config.Insecure,
	})
	if err != nil {
		return nil, err
	}
	return &s3Bucket{
		client: &client{ref: minioClient},
		bucket: config.Bucket,
	}, nil
}

type s3Bucket struct {
	client s3Access
	bucket string
}

// Iter implements bucket.Reader
func (b *s3Bucket) Iter(
	ctx context.Context, dir string, f func(string) error, options bucket.IterOptions,
) error {
	// Ensure the object name actually ends with a dir suffix. Otherwise we'll just iterate the
	// object itself as one prefix item.
	if dir != "" {
		dir = strings.TrimSuffix(dir, DirDelim) + DirDelim
	}
	after := strings.TrimPrefix(options.StartAfter, b.bucket+DirDelim)
	opts := minio.ListObjectsOptions{
		Prefix:     dir,
		MaxKeys:    options.Max,
		Recursive:  options.Recursive,
		StartAfter: after,
		UseV1:      false,
	}
	log.Infof("Iter: bucket: %q, after: %q", b.bucket, after)
	for object := range b.client.ListObjects(ctx, b.bucket, opts) {
		if object.Err != nil {
			return object.Err
		}
		if object.Key == "" || object.Key == dir {
			continue
		}
		if err := f(object.Key); err != nil {
			return err
		}
	}

	return ctx.Err()
}

// Get implements bucket.Reader
func (b *s3Bucket) Get(ctx context.Context, file string) (io.ReadCloser, error) {
	log.Infof("Get: %q", file)
	file = strings.TrimPrefix(file, b.bucket+DirDelim)
	return b.client.GetObject(ctx, b.bucket, file, minio.GetObjectOptions{})
}
