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
	"math"
	"net/url"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/cockroachdb/replicator/internal/conveyor"
	"github.com/cockroachdb/replicator/internal/script"
	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/sinkprod"
	"github.com/cockroachdb/replicator/internal/source/objstore/providers/s3"
	"github.com/cockroachdb/replicator/internal/target/dlq"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/secure"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

const defaultEndpoint = "https://s3.amazonaws.com"

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

// PartitionFormat identifies how files are organized within
// a bucket: daily, hourly or flat formats.
type PartitionFormat int

//go:generate go run golang.org/x/tools/cmd/stringer -type=PartitionFormat

const (
	// Daily is the default option:
	// /[date]/[timestamp]-[uniquer]-[topic]-[schema-id]
	Daily PartitionFormat = iota
	// Flat result in no file partitioning:
	// /[timestamp]-[uniquer]-[topic]-[schema-id]
	Flat
	// Hourly will partition into an hourly directory:
	// /[date]/[hour]/[timestamp]-[uniquer]-[topic]-[schema-id]
	Hourly
)

// PartitionFormats returns the available partition formats.
func PartitionFormats() []string {
	res := make([]string, 0)
	for i := Daily; i <= Hourly; i++ {
		res = append(res, i.String())
	}
	return res
}

var _ pflag.Value = new(PartitionFormat)

// Set implements pflag.Value
func (p *PartitionFormat) Set(value string) error {
	switch strings.ToLower(value) {
	case "":
		// default partition format, when nothing is specified.
		*p = PartitionFormat(Daily)
	case "daily":
		*p = PartitionFormat(Daily)
	case "flat":
		*p = PartitionFormat(Flat)
	case "hourly":
		*p = PartitionFormat(Hourly)
	default:
		return errors.Errorf("invalid partition format %q", value)
	}
	return nil
}

// Type implements pflag.Value
func (p PartitionFormat) Type() string {
	return fmt.Sprintf("%T", p)
}

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
	MaxTimestamp         hlc.Time
	MinTimestamp         hlc.Time
	PartitionFormat      PartitionFormat
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
	timeRange  hlc.Range // Timestamp range, computed based on minTimestamp and maxTimestamp.
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
	f.Var(&c.MaxTimestamp, "maxTimestamp",
		`only accept messages older than this timestamp; this is an exclusive upper limit.
The timestamp must be provided in the HLC timestamp format, as returned by
cluster_logical_timestamp().`)
	f.Var(&c.MinTimestamp, "minTimestamp",
		`"only accept unprocessed messages at or newer than this timestamp; this is an inclusive lower limit.
The timestamp must be provided in the HLC timestamp format, as returned by
cluster_logical_timestamp().`)
	f.Var(&c.PartitionFormat, "partitionFormat",
		fmt.Sprintf("how changefeed file paths are partitioned: %s",
			strings.Join(PartitionFormats(), ", ")))
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
	maxTimestamp := hlc.New(math.MaxInt64, math.MaxInt)
	if hlc.Compare(c.MaxTimestamp, hlc.Zero()) > 0 {
		maxTimestamp = c.MaxTimestamp
	}
	if hlc.Compare(c.MinTimestamp, maxTimestamp) > 0 {
		return errors.New("minTimestamp must be before maxTimestamp")
	}
	c.timeRange = hlc.RangeExcluding(c.MinTimestamp, maxTimestamp)
	switch Providers[u.Scheme] {
	case LocalStorage:
		c.local = os.DirFS(u.Path)
		c.identifier = fmt.Sprintf("objstore:file///%s", u.Path)
	case S3Storage:
		if u.Host != "" {
			c.bucketName = u.Host
			// Extract provider configuration from the storage URL
			c.prefix = strings.TrimPrefix(u.Path, "/")
		} else {
			return errors.New("missing bucket name in URL. Must be s3://bucket/folder")
		}
		c.identifier = fmt.Sprintf("objstore:%s//%s/%s", u.Scheme, u.Host, u.Path)
		params := u.Query()
		endpointURL := paramValue(params, "AWS_ENDPOINT")
		// The minio API require a endpoint to be set.
		// We will be using AWS S3 as the default.
		if endpointURL == "" {
			endpointURL = defaultEndpoint
		}
		endpointParsed, err := url.Parse(endpointURL)
		if err != nil {
			return err
		}
		c.s3 = &s3.Config{
			AccessKey:    paramValue(params, "AWS_ACCESS_KEY_ID"),
			Bucket:       c.bucketName,
			Endpoint:     endpointParsed.Host,
			Insecure:     endpointParsed.Scheme == "http",
			SecretKey:    paramValue(params, "AWS_SECRET_ACCESS_KEY"),
			SessionToken: paramValue(params, "AWS_SESSION_TOKEN"),
		}
	default:
		return errors.Errorf("unknown scheme %s", u.Scheme)
	}
	return nil
}

// paramValue gets the value for the specified parameter from the URL.
// If not present in the URL, it retrieves a value from the environment.
func paramValue(params url.Values, key string) string {
	value := params.Get(key)
	if value != "" {
		return value
	}
	return os.Getenv(key)
}
