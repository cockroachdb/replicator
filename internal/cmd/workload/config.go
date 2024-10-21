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

package workload

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/source/cdc/server"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/auth/jwt"
	"github.com/cockroachdb/replicator/internal/util/auth/trust"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/stdlogical"
	"github.com/cockroachdb/replicator/internal/util/workload"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
)

const (
	defaultBatchSize          = 1024
	defaultConcurrentRequests = 128
	defaultRate               = 10
	defaultRequestTimeout     = 30 * time.Second
	defaultResolvedInterval   = time.Second
	defaultRetryMax           = 30 * time.Second
	defaultRetryMin           = 100 * time.Millisecond
	defaultURL                = "https://127.0.0.1:26258/cdc_workload/public?insecure_tls_skip_verify=true"
)

type clientConfig struct {
	batchSize          int
	concurrentRequests int
	failFast           bool
	metricsAddr        string
	rate               int
	requestTimeout     time.Duration
	resolvedInterval   time.Duration
	retryMin, retryMax time.Duration
	suffix             string

	childTable   ident.Table  // Derived from targetSchema
	parentTable  ident.Table  // Derived from targetSchema
	targetSchema ident.Schema // Extracted from url.
	token        string       // Bearer token.
	url          string       // Destination URL.
	urlParsed    *url.URL     // Parsed version of url.
}

func (c *clientConfig) Bind(flags *pflag.FlagSet) {
	flags.IntVar(&c.batchSize, "batchSize", defaultBatchSize,
		"the number of updates within a batch")
	flags.IntVar(&c.concurrentRequests, "concurrentRequests", defaultConcurrentRequests,
		"the maximum number of concurrent HTTP requests")
	flags.BoolVar(&c.failFast, "failFast", false,
		"exit immediately if any error is encountered")
	flags.IntVar(&c.rate, "rateLimit", defaultRate,
		"the number of source transaction per second to emit")
	flags.DurationVar(&c.requestTimeout, "requestTimeout", defaultRequestTimeout,
		"HTTP request timeout")
	flags.DurationVar(&c.resolvedInterval, "resolvedInternal", defaultResolvedInterval,
		"the approximate time between resolved timestamp messages")
	flags.DurationVar(&c.retryMax, "retryMax", defaultRetryMax,
		"the maximum delay between HTTP retry attempts")
	flags.DurationVar(&c.retryMin, "retryMin", defaultRetryMin,
		"the minimum delay between HTTP retry attempts")
	flags.StringVar(&c.suffix, "suffix", "",
		"parent/child table suffix")
	flags.StringVar(&c.token, "token", "",
		"JWT bearer token if security is enabled")
	flags.StringVar(&c.url, "url", defaultURL,
		"the destination URL; add insecure_tls_skip_verify=true to disable TLS checks")
}

func (c *clientConfig) Preflight() error {
	if c.batchSize == 0 {
		c.batchSize = defaultBatchSize
	}
	if c.concurrentRequests == 0 {
		c.concurrentRequests = defaultConcurrentRequests
	}
	if c.rate == 0 {
		c.rate = defaultRate
	}
	if c.requestTimeout == 0 {
		c.requestTimeout = defaultRequestTimeout
	}
	if c.resolvedInterval == 0 {
		c.resolvedInterval = defaultResolvedInterval
	}
	if c.retryMax == 0 {
		c.retryMax = defaultRetryMax
	}
	if c.retryMin == 0 {
		c.retryMin = defaultRetryMin
	}
	if c.urlParsed == nil {
		if c.url == "" {
			return errors.New("no URL provided")
		}
		// token may be empty
		var err error
		if c.urlParsed, err = url.Parse(c.url); err != nil {
			return errors.Wrapf(err, "invalid url: %q", c.url)
		}
	}
	// Extract the schema from the path
	if c.targetSchema.Empty() {
		var err error
		c.targetSchema, err = ident.ParseSchema(strings.ReplaceAll(c.urlParsed.Path[1:], "/", "."))
		if err != nil {
			return errors.Wrapf(err, "could not extract a schema name from URL path %q",
				c.urlParsed.Path)
		}
	}
	if c.childTable.Empty() {
		c.childTable = ident.NewTable(c.targetSchema, ident.New("child"+c.suffix))
	}
	if c.parentTable.Empty() {
		c.parentTable = ident.NewTable(c.targetSchema, ident.New("parent"+c.suffix))
	}
	return nil
}

func (c *clientConfig) createTables(ctx *stopper.Context, targetPool *types.TargetPool) error {
	// We need a 64-bit type.
	bigType := "BIGINT"
	if c.suffix == "" {
		c.suffix = fmt.Sprintf("_%d_%d", os.Getpid(), rand.Int32N(10000))
	}

	// Create the tables within the "current" schema specified on the
	// command-line. We'll use uniquely-named tables to ensure that
	// repeated runs do something sane.
	var current string
	switch targetPool.Product {
	case types.ProductCockroachDB, types.ProductPostgreSQL:
		if err := targetPool.QueryRow("SELECT current_database()").Scan(&current); err != nil {
			return errors.WithStack(err)
		}

	case types.ProductMariaDB, types.ProductMySQL:
		if err := targetPool.QueryRow("SELECT database()").Scan(&current); err != nil {
			return errors.WithStack(err)
		}

	case types.ProductOracle:
		if err := targetPool.QueryRow("SELECT user FROM dual").Scan(&current); err != nil {
			return errors.WithStack(err)
		}
		bigType = "NUMBER(38)"

	default:
		return errors.Errorf(
			"the demo subcommand does not support creating a target schema within %s; "+
				"create the schema manually, start a Replicator server, and then use the "+
				"run subcommand instead.", targetPool.Product)
	}

	var err error
	c.targetSchema, err = targetPool.Product.ExpandSchema(ident.MustSchema(ident.New(current)))
	if err != nil {
		return err
	}
	c.parentTable = ident.NewTable(c.targetSchema, ident.New("parent"+c.suffix))
	c.childTable = ident.NewTable(c.targetSchema, ident.New("child"+c.suffix))

	if _, err := targetPool.ExecContext(ctx, fmt.Sprintf(
		`CREATE TABLE %s(parent %[2]s PRIMARY KEY, val %[2]s DEFAULT 0 NOT NULL)`,
		c.parentTable, bigType)); err != nil {
		return errors.WithStack(err)
	}

	if _, err := targetPool.ExecContext(ctx, fmt.Sprintf(
		`CREATE TABLE %[1]s (
child %[4]s PRIMARY KEY,
parent %[4]s NOT NULL,
val %[4]s DEFAULT 0 NOT NULL,
CONSTRAINT parent_fk%[2]s FOREIGN KEY(parent) REFERENCES %[3]s(parent)
)`, c.childTable, c.suffix, c.parentTable, bigType)); err != nil {
		return errors.WithStack(err)
	}

	log.Infof("parent table: %s, child table: %s", c.parentTable, c.childTable)
	return nil
}

// generateJWT injects a testing key into the server.
func (c *clientConfig) generateJWT(ctx context.Context, svr *server.Server) error {
	method, priv, err := jwt.InsertTestingKey(ctx, svr.StagingPool,
		svr.GetAuthenticator(), svr.StagingSchema)
	if err != nil {
		return err
	}
	_, token, err := jwt.Sign(method, priv, []ident.Schema{c.targetSchema})
	if err != nil {
		return err
	}
	c.token = token
	return nil
}

// initURL initializes the URL from the listener.
func (c *clientConfig) initURL(svr net.Listener) error {
	c.url = fmt.Sprintf("https://%s/%s?insecure_tls_skip_verify=true",
		svr.Addr(), ident.Join(c.targetSchema, ident.Raw, '/'))
	return nil
}

// newRunner updates the configuration to drive the server using the
// given workload generator. A runner is returned.
func (c *clientConfig) newRunner(
	ctx *stopper.Context, gen *workload.GeneratorBase,
) (*runner, error) {
	if err := c.Preflight(); err != nil {
		return nil, err
	}

	// Create workload generator if not being driven from test code.
	if gen == nil {
		gen = workload.NewGeneratorBase(c.parentTable, c.childTable)
	}

	return newRunner(ctx, c, gen)
}

// newServer starts a server and returns it.
func (c *clientConfig) newServer(
	ctx *stopper.Context, serverCfg *server.Config,
) (*server.Server, error) {
	if err := serverCfg.Preflight(); err != nil {
		return nil, err
	}

	svr, err := server.NewServer(ctx, serverCfg)
	if err != nil {
		return nil, err
	}
	stdlogical.AddHandlers(svr.GetAuthenticator(), svr.GetServeMux(), svr.GetDiagnostics())
	log.Infof("server listening on %s", svr.GetListener().Addr())

	if c.metricsAddr != "" {
		cancel, err := stdlogical.MetricsServer(trust.New(), c.metricsAddr, svr.GetDiagnostics())
		if err != nil {
			return nil, err
		}
		ctx.Defer(cancel)
	}

	return svr, nil
}
