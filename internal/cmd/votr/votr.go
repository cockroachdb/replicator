// Copyright 2023 The Cockroach Authors
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

// Package votr contains a demonstration workload.
package votr

import (
	"context"
	"embed"
	"fmt"
	"math"
	"math/rand"
	"net"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/source/cdc"
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/source/server"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/cockroachdb/cdc-sink/internal/util/stdpool"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/cockroachdb/cdc-sink/internal/util/subfs"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type region int

const (
	east region = iota
	west
)

func (r region) String() string {
	switch r {
	case east:
		return "east"
	case west:
		return "west"
	default:
		return fmt.Sprintf("region(%d)", r)
	}
}

type config struct {
	BallotBatch     int
	Candidates      int
	ConnectEast     string
	ConnectWest     string
	Enclosing       ident.Ident
	ReportInterval  time.Duration
	SinkHost        string
	StopAfter       time.Duration
	ValidationDelay time.Duration
	WorkerDelay     time.Duration
	Workers         int
}

func (c *config) Bind(f *pflag.FlagSet) {
	f.IntVar(&c.BallotBatch, "ballotBatch", 10,
		"the number of ballots to record in a single batch")
	f.IntVar(&c.Candidates, "candidates", 16,
		"the number of candidate rows")
	f.StringVar(&c.ConnectEast, "connectEast",
		"postgresql://root@localhost:26257/?sslmode=disable",
		"a CockroachDB connection string")
	f.StringVar(&c.ConnectWest, "connectWest",
		"postgresql://root@localhost:26258/?sslmode=disable",
		"a CockroachDB connection string")
	f.Var(ident.NewValue("votr", &c.Enclosing), "schema",
		"the enclosing database schema")
	f.DurationVar(&c.ReportInterval, "reportiAfter", 5*time.Second,
		"report number of ballots inserted")
	f.StringVar(&c.SinkHost, "sinkHost", "127.0.0.1",
		"the hostname to use when creating changefeeds")
	f.DurationVar(&c.StopAfter, "stopAfter", 0,
		"if non-zero, exit after running for this long")
	f.DurationVar(&c.ValidationDelay, "validationDelay", 15*time.Second,
		"sleep time between validation cycles")
	f.DurationVar(&c.WorkerDelay, "workerDelay", 100*time.Millisecond,
		"sleep time between ballot stuffing")
	f.IntVar(&c.Workers, "workers", 8,
		"the number of concurrent ballot stuffers")
}

// Command returns the VOTR workload.
func Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "votr",
		Short: "a workload to demonstrate async, two-way replication",
	}
	cmd.AddCommand(commandInit(), commandRun())
	return cmd
}

func commandInit() *cobra.Command {
	cfg := &config{}
	cmd := &cobra.Command{
		Use:   "init",
		Short: "initialize the VOTR schema",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			east, cancel, err := openSchema(ctx, cfg, east)
			if err != nil {
				return errors.Wrap(err, "could not connect to east target database")
			}
			defer cancel()
			if err := east.create(ctx); err != nil {
				return errors.Wrap(err, "could not create east VOTR schema")
			}

			west, cancel, err := openSchema(ctx, cfg, west)
			if err != nil {
				return errors.Wrap(err, "could not connect to west target database")
			}
			defer cancel()
			if err := west.create(ctx); err != nil {
				return errors.Wrap(err, "could not create west VOTR schema")
			}

			return nil
		},
	}
	cfg.Bind(cmd.Flags())
	return cmd
}

func commandRun() *cobra.Command {
	cfg := &config{}
	cmd := &cobra.Command{
		Use:   "run",
		Short: "run the VOTR workload",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			// Create a detached stopper so we can control shutdown.
			ctx := stopper.WithContext(context.Background())

			// This goroutine will stop the workload in response to
			// being interrupted or if the test duration has elapsed.
			ctx.Go(func() error {
				delay := cfg.StopAfter
				if delay == 0 {
					delay = math.MaxInt64
				}
				select {
				case <-ctx.Stopping():
				case <-cmd.Context().Done():
					log.Infof("shutdown signal received")
				case <-time.After(delay):
					log.Info("stopping workload after configured time")
				}
				ctx.Stop(30 * time.Second)
				return nil
			})

			// Run the requested number of workers.
			log.Infof("inserting with %d workers across %d candidates", cfg.Workers, cfg.Candidates)
			if cfg.WorkerDelay > 0 {
				log.Infof("theoretical max regional ballots per reporting interval: %d",
					int64(cfg.Workers)*int64(cfg.BallotBatch)*
						cfg.ReportInterval.Nanoseconds()/cfg.WorkerDelay.Nanoseconds())
				log.Info("low performance may indicate contention due to too few candidates")
			}

			eastDB, eastSink, cancel, err := worker(ctx, cfg, east)
			if err != nil {
				return err
			}
			defer cancel()
			log.Infof("east sink is %s", eastSink)

			westDB, westSink, cancel, err := worker(ctx, cfg, west)
			if err != nil {
				return err
			}
			defer cancel()
			log.Infof("west sink is %s", westSink)

			if err := createFeed(ctx, eastDB, westSink); err != nil {
				return err
			}

			if err := createFeed(ctx, westDB, eastSink); err != nil {
				return err
			}

			return ctx.Wait()
		},
	}
	cfg.Bind(cmd.Flags())
	return cmd
}

// createFeed creates a changefeed from the given source to the given
// server.
func createFeed(ctx *stopper.Context, from *schema, to *url.URL) error {
	// Set the cluster settings once, if we need to.
	var enabled bool
	if err := retry.Retry(ctx, func(ctx context.Context) error {
		return from.db.QueryRowContext(ctx, "SHOW CLUSTER SETTING kv.rangefeed.enabled").Scan(&enabled)
	}); err != nil {
		return errors.Wrap(err, "could not check cluster setting")
	}
	if !enabled {
		if err := retry.Retry(ctx, func(ctx context.Context) error {
			_, err := from.db.ExecContext(ctx, "SET CLUSTER SETTING kv.rangefeed.enabled = true")
			return errors.Wrapf(err, "%s: could not enable rangefeeds", from.region)
		}); err != nil {
			return err
		}
	}

	lic, hasLic := os.LookupEnv("COCKROACH_DEV_LICENSE")
	org, hasOrg := os.LookupEnv("COCKROACH_DEV_ORGANIZATION")
	if hasLic && hasOrg {
		if err := retry.Retry(ctx, func(ctx context.Context) error {
			_, err := from.db.ExecContext(ctx, "SET CLUSTER SETTING enterprise.license = $1", lic)
			return errors.Wrapf(err, "%s: could not set cluster license", from.region)
		}); err != nil {
			return err
		}

		if err := retry.Retry(ctx, func(ctx context.Context) error {
			_, err := from.db.ExecContext(ctx, "SET CLUSTER SETTING cluster.organization = $1", org)
			return errors.Wrapf(err, "%s: could not set cluster organization", from.region)
		}); err != nil {
			return err
		}
	}

	q := fmt.Sprintf(`CREATE CHANGEFEED FOR TABLE %s, %s, %s INTO '%s'
WITH diff, updated, resolved='1s', min_checkpoint_frequency='1s', initial_scan='no'`,
		from.ballots, from.candidates, from.totals, to.String(),
	)

	if err := retry.Retry(ctx, func(ctx context.Context) error {
		_, err := from.db.ExecContext(ctx, q)
		return errors.Wrapf(err, "%s: could not create changefeed", from.region)
	}); err != nil {
		return err
	}

	log.Infof("created feed from %s into %s", from.region, to)
	return nil
}

func openSchema(ctx context.Context, cfg *config, r region) (*schema, func(), error) {
	var conn string
	switch r {
	case east:
		conn = cfg.ConnectEast
	case west:
		conn = cfg.ConnectWest
	default:
		return nil, nil, errors.Errorf("%s: unimplemented", r)
	}

	pool, cancel, err := stdpool.OpenPgxAsTarget(ctx, conn,
		stdpool.WithConnectionLifetime(5*time.Minute),
		stdpool.WithPoolSize(cfg.Workers+1),
		stdpool.WithTransactionTimeout(time.Minute),
	)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "could not connect to %s database", r)
	}

	return newSchema(pool.DB, cfg.Enclosing, r), cancel, nil
}

//go:embed script/*
var scriptFS embed.FS

// startServer runs an instance of cdc-sink which will feed into
// the given destination. It returns the base URL for delivering
// messages to the sink.
func startServer(ctx *stopper.Context, cfg *config, dest *schema) (*url.URL, func(), error) {
	var targetConn string
	switch dest.region {
	case east:
		targetConn = cfg.ConnectEast
	case west:
		targetConn = cfg.ConnectWest
	default:
		return nil, nil, errors.Errorf("unimplemented: %s", dest.region)
	}

	stagingName := ident.New(fmt.Sprintf("cdc_sink_%d_%s", os.Getpid(), dest.region))
	stagingSchema := ident.MustSchema(dest.enclosing, stagingName)

	if _, err := dest.db.ExecContext(ctx, fmt.Sprintf(
		`CREATE SCHEMA IF NOT EXISTS %s`, stagingSchema)); err != nil {
		return nil, nil, errors.Wrap(err, dest.region.String())
	}

	srvConfig := &server.Config{
		CDC: cdc.Config{
			BaseConfig: logical.BaseConfig{
				BackfillWindow:     24 * time.Hour,
				ForeignKeysEnabled: true,
				ScriptConfig: script.Config{
					FS: &subfs.SubstitutingFS{
						FS: scriptFS,
						Replacer: strings.NewReplacer(
							"{{DEST}}", dest.region.String(),
						),
					},
					MainPath: "/script/votr.ts",
				},
				StagingConn:   targetConn,
				StagingSchema: stagingSchema,
				TargetConn:    targetConn,
			},
			MetaTableName:       ident.New("resolved_timestamps"),
			FlushEveryTimestamp: true, // Needed for delta behavior.
			IdealFlushBatchSize: 1,    // Need fine-grained transactions to avoid contention.
		},
		BindAddr:    ":0",
		DisableAuth: true,
	}
	if err := srvConfig.Preflight(); err != nil {
		return nil, nil, errors.Wrap(err, dest.region.String())
	}
	srv, cancel, err := server.NewServer(ctx, srvConfig)
	if err != nil {
		return nil, nil, errors.Wrap(err, dest.region.String())
	}
	_, port, err := net.SplitHostPort(srv.GetAddr().String())
	if err != nil {
		cancel()
		return nil, nil, errors.Wrap(err, dest.region.String())
	}

	sink := &url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%s:%s", cfg.SinkHost, port),
		Path:   ident.Join(dest.candidates.Schema(), ident.Raw, '/'),
	}
	return sink, cancel, nil
}

// worker will launch a number of goroutines into the context.
// It returns the address of a cdc-sink server that accepts writes
// to the given region.
func worker(ctx *stopper.Context, cfg *config, r region) (*schema, *url.URL, func(), error) {
	sch, cancel, err := openSchema(ctx, cfg, r)
	if err != nil {
		return nil, nil, nil, err
	}

	if err := sch.ensureCandidates(ctx, cfg.Candidates); err != nil {
		cancel()
		return nil, nil, nil, errors.Wrapf(err, "%s: could not create candidate entries", r)
	}

	warnings, err := sch.validate(ctx, false)
	if err != nil {
		cancel()
		return nil, nil, nil, errors.Wrapf(err, "%s: could not perform initial validation", r)
	}
	if len(warnings) > 0 {
		log.WithField(
			"inconsistent", warnings,
		).Warnf("%s: workload starting from inconsistent state", r)
	}

	ballotsInserted := &atomic.Int64{}
	for i := 0; i < cfg.Workers; i++ {
		ctx.Go(func() error {
			// Stagger start by a random amount.
			sleep := time.Duration(rand.Int63n(cfg.WorkerDelay.Nanoseconds()))

			for {
				select {
				case <-time.After(sleep):
					sleep = cfg.WorkerDelay
				case <-ctx.Stopping():
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}

				if err := sch.doStuff(ctx, cfg.BallotBatch); err != nil {
					return errors.Wrap(err, "could not stuff ballots")
				}
				ballotsInserted.Add(int64(cfg.BallotBatch))
			}
		})
	}

	// Print status.
	ctx.Go(func() error {
		for {
			select {
			case <-time.After(cfg.ReportInterval):
				log.Infof("%s: inserted %d ballots", r, ballotsInserted.Swap(0))
			case <-ctx.Stopping():
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	// Start a background validation loop.
	ctx.Go(func() error {
		for {
			select {
			case <-time.After(cfg.ValidationDelay):
			case <-ctx.Stopping():
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}

			warnings, err := sch.validate(ctx, true)
			if err != nil {
				return errors.Wrap(err, "could not validate results")
			}
			if len(warnings) == 0 {
				log.Infof("%s: workload is consistent", r)
				continue
			}
			log.WithField(
				"inconsistent", warnings,
			).Warnf("%s: workload in inconsistent state", r)
		}
	})

	svr, svrCancel, err := startServer(ctx, cfg, sch)
	if err != nil {
		cancel()
		return nil, nil, nil, err
	}

	return sch, svr, func() {
		svrCancel()
		cancel()
	}, nil
}
