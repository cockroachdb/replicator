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

package objstore

import (
	"bufio"
	"context"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path"
	"runtime"
	"strings"
	"time"

	"github.com/cockroachdb/replicator/internal/conveyor"
	"github.com/cockroachdb/replicator/internal/script"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/sinkprod"
	"github.com/cockroachdb/replicator/internal/source/objstore/providers/s3"
	"github.com/cockroachdb/replicator/internal/target/dlq"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/secure"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

var (
	defaultBufferSize           = bufio.MaxScanTokenSize // 64K
	defaultFetchDelay           = 100 * time.Millisecond
	defaultNumberOfWorkers      = runtime.GOMAXPROCS(0)
	defaultRetryInitialInterval = 10 * time.Millisecond
	defaultRetryMaxTime         = 10 * time.Second
)

// EagerConfig is a hack to get Wire to move userscript evaluation to
// the beginning of the injector. This allows CLI flags to be set by the
// script.
type EagerConfig Config

// Provider identifies the type of providers.
//
//go:generate go run golang.org/x/tools/cmd/stringer -type=Provider
type Provider int

const (
	// UnknownStorage identifies other storage not currently supported.
	UnknownStorage Provider = iota
	// LocalStorage identifies a object stored backed by local storage.
	LocalStorage
	// S3Storage identifies a object stored backed by AWS S3.
	S3Storage
)

// Providers maps a URL scheme to a Provider.
var Providers = map[string]Provider{
	"file": LocalStorage,
	"s3":   S3Storage,
}

// Config contains the configuration necessary for creating a
// connection to an object store.
type Config struct {
	Conveyor  conveyor.Config
	DLQ       dlq.Config
	Script    script.Config
	Sequencer sequencer.Config
	Staging   sinkprod.StagingConfig
	Target    sinkprod.TargetConfig
	TLS       secure.Config

	// Object store specific configuration
	BufferSize           int
	FetchDelay           time.Duration
	RetryInitialInterval time.Duration
	RetryMaxTime         time.Duration
	StorageURL           string
	TargetSchema         ident.Schema
	Workers              int

	// The following are computed
	bucketName string
	identifier string // used for leasing and state.
	local      fs.FS
	prefix     string
	s3         *s3.Config
}

// Bind adds flags to the set. It delegates to the embedded Config.Bind.
func (c *Config) Bind(f *pflag.FlagSet) {
	c.Conveyor.Bind(f)
	c.DLQ.Bind(f)
	c.Script.Bind(f)
	c.Sequencer.Bind(f)
	c.Staging.Bind(f)
	c.Target.Bind(f)
	c.TLS.Bind(f)

	f.IntVar(&c.BufferSize, "bufferSize", defaultBufferSize,
		"buffer size for the ndjson parser")
	f.DurationVar(&c.FetchDelay, "fetchDelay", defaultFetchDelay,
		"time to wait between fetching the list of entries in a bucket")
	f.DurationVar(&c.RetryInitialInterval, "retryInitial", defaultRetryInitialInterval,
		"initial time to wait before retrying an operation that failed because of a transient error")
	f.DurationVar(&c.RetryMaxTime, "retryMax", defaultRetryMaxTime,
		"maximum time allowed for retrying an operation that failed because of a transient error")
	f.StringVar(&c.StorageURL, "storageURL", "", "the URL to access the storage")
	f.Var(ident.NewSchemaFlag(&c.TargetSchema), "targetSchema",
		"the SQL database schema in the target cluster to update")
	f.IntVar(&c.Workers, "workers", defaultNumberOfWorkers,
		"maximum number of workers to process mutation files")
	if err := f.MarkHidden(sequencer.AssumeIdempotent); err != nil {
		panic(err)
	}
}

// Preflight updates the configuration with sane defaults or returns an
// error if there are missing options for which a default cannot be
// provided.
func (c *Config) Preflight(ctx context.Context) error {
	if err := c.DLQ.Preflight(); err != nil {
		return err
	}
	if err := c.Script.Preflight(); err != nil {
		return err
	}
	if err := c.Sequencer.Preflight(); err != nil {
		return err
	}
	if err := c.Staging.Preflight(); err != nil {
		return err
	}
	if err := c.Target.Preflight(); err != nil {
		return err
	}
	if err := c.TLS.Preflight(); err != nil {
		return err
	}
	if c.TargetSchema.Empty() {
		return errors.New("no target schema specified")
	}
	// We can disable idempotent tracking in the sequencer stack
	// since the logical stream is idempotent.
	c.Sequencer.IdempotentSource = true
	return c.preflight()
}

func (c *Config) preflight() error {
	u, err := url.Parse(c.StorageURL)
	if err != nil {
		return err
	}
	// Extract provider configuration from the storage URL
	c.bucketName = strings.TrimPrefix(u.Path, "/")
	elements := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	c.bucketName = elements[0]
	if len(elements) > 1 {
		c.prefix = path.Join(elements[1:]...)
	}
	c.identifier = fmt.Sprintf("objstore:%s//%s/%s", u.Scheme, c.bucketName, c.prefix)
	switch Providers[u.Scheme] {
	case LocalStorage:
		c.local = os.DirFS(u.Path)
	case S3Storage:
		params := u.Query()
		ep, err := url.Parse(params.Get("AWS_ENDPOINT"))
		if err != nil {
			return err
		}
		c.s3 = &s3.Config{
			AccessKey: params.Get("AWS_ACCESS_KEY_ID"),
			Bucket:    c.bucketName,
			Endpoint:  ep.Host,
			Insecure:  ep.Scheme == "http",
			SecretKey: params.Get("AWS_SECRET_ACCESS_KEY"),
		}
	default:
		return errors.Errorf("unknown scheme %s", u.Scheme)
	}
	return nil
}
