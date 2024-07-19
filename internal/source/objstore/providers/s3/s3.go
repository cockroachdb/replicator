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

// Package s3 provides access to AWS S3 bucket. This is not a generic
// abstract layer, but it rather focuses on accessing CockroachDB
// changefeed events stored in a S3 bucket.
package s3

import (
	"context"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/source/objstore/bucket"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const (
	// Delimiter is the folder delimiter used. While Amazon S3 has a flat
	// structure instead of a hierarchy, for the sake of organizational
	// simplicity, the Amazon S3 console supports the folder concept as
	// a means of grouping objects, where a shared name prefix is used
	// for the grouped objects. In the context of CockroachDB we will be
	// using "/" to define folder boundaries, mimicking the familiar
	// organization within a Unix filesystem.
	Delimiter = "/"
)

var (
	// RetriableErrors identifies errors that are transient. The
	// operation causing the error may be retried.
	RetriableErrors = []int{
		http.StatusBadGateway,
		http.StatusGatewayTimeout,
		http.StatusInternalServerError,
		http.StatusTooManyRequests,
	}
)

// Config has the parameters used to connect to S3.
type Config struct {
	AccessKey    string // AWS Access Key
	Bucket       string // The name of the bucket.
	Endpoint     string // Alternative server to use, for other S3 providers.
	Insecure     bool   // For testing against self hosted S3 providers.
	SecretKey    string // Secret associated to the Access Key
	SessionToken string // Session token
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
func New(config *Config) (bucket.Bucket, error) {
	minioClient, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.AccessKey, config.SecretKey, config.SessionToken),
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

// Iter implements bucket.Bucket
func (b *s3Bucket) Walk(
	ctx *stopper.Context,
	dir string,
	options *bucket.WalkOptions,
	f func(*stopper.Context, string) error,
) error {
	// Ensure the object name actually ends with a dir suffix. Otherwise we'll just iterate the
	// object itself as one prefix item.
	if dir != "" {
		dir = strings.TrimSuffix(dir, Delimiter) + Delimiter
	}
	after := strings.TrimPrefix(options.StartAfter, b.bucket+Delimiter)
	opts := minio.ListObjectsOptions{
		Prefix:     dir,
		MaxKeys:    options.Limit,
		Recursive:  options.Recursive,
		StartAfter: after,
		UseV1:      false,
	}
	childCtx, cancelChild := context.WithCancel(ctx)
	// From ListObjects docs: If caller cancels the context, then the last entry
	// on the 'chan ObjectInfo' will be the context.Error() caller must
	// drain the channel entirely and wait until channel is closed
	// before proceeding, without waiting on the channel to be closed
	// completely you might leak goroutines.
	objects := b.client.ListObjects(childCtx, b.bucket, opts)
	defer func() {
		// cancel the child context to signal ListObjects that
		// we are done.
		cancelChild()
		// drain the channel to avoid goroutine leaks.
		for range objects {
		}
	}()
	for object := range objects {
		if object.Err != nil {
			return object.Err
		}
		if object.Key == "" || object.Key == dir {
			continue
		}
		if err := f(ctx, object.Key); err != nil {
			if errors.Is(err, bucket.ErrSkipAll) {
				return nil
			}
			return err
		}
	}
	return nil
}

// Open implements bucket.Bucket
func (b *s3Bucket) Open(ctx *stopper.Context, file string) (io.ReadCloser, error) {
	file = strings.TrimPrefix(file, b.bucket+Delimiter)
	r, err := b.client.GetObject(ctx, b.bucket, file, minio.GetObjectOptions{})
	if err != nil {
		resp := minio.ToErrorResponse(err)
		if resp.StatusCode == http.StatusNotFound {
			return nil, errors.Join(bucket.ErrNoSuchKey, err)
		}
		if slices.Contains(RetriableErrors, resp.StatusCode) {
			return nil, errors.Join(bucket.ErrTransient, err)
		}
	}
	return r, err
}
