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
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/source/cdc"
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/source/server"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/cockroachdb/cdc-sink/internal/util/stdpool"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/cockroachdb/cdc-sink/internal/util/subfs"
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type region int

func (r region) String() string {
	return strconv.Itoa(int(r))
}

type config struct {
	BallotBatch     int
	Candidates      int
	Connect         []string
	DrainDelay      time.Duration
	Enclosing       ident.Ident
	ErrInconsistent bool
	MaxBallots      int
	ReportInterval  time.Duration
	SinkHost        string
	StopAfter       time.Duration
	ValidationDelay time.Duration
	WorkerDelay     time.Duration
	Workers         int

	schemas   []*schema // Used by test code to post-validate.
	remaining atomic.Int64
}

func (c *config) Bind(f *pflag.FlagSet) {
	f.IntVar(&c.BallotBatch, "ballotBatch", 10,
		"the number of ballots to record in a single batch")
	f.IntVar(&c.Candidates, "candidates", 128,
		"the number of candidate rows")
	f.StringSliceVar(&c.Connect, "connect",
		[]string{
			"postgresql://root@localhost:26257/?sslmode=disable",
			"postgresql://root@localhost:26258/?sslmode=disable",
		},
		"two or more CockroachDB connection strings")
	f.DurationVar(&c.DrainDelay, "drainDelay", time.Minute,
		"pause between stopping workload and stopping cdc-sink processes")
	f.Var(ident.NewValue("votr", &c.Enclosing), "schema",
		"the enclosing database schema")
	f.BoolVar(&c.ErrInconsistent, "errorIfInconsistent", false,
		"exit immediately if the database is inconsistent")
	f.IntVar(&c.MaxBallots, "maxBallots", 0,
		"if non-zero, the total number of ballots to be cast")
	f.DurationVar(&c.ReportInterval, "reportAfter", 5*time.Second,
		"report number of ballots inserted")
	f.StringVar(&c.SinkHost, "sinkHost", "127.0.0.1",
		"the hostname to use when creating changefeeds")
	f.DurationVar(&c.StopAfter, "stopAfter", 0,
		"if non-zero, exit after running for this long")
	f.DurationVar(&c.ValidationDelay, "validationDelay", 15*time.Second,
		"sleep time between validation cycles")
	f.DurationVar(&c.WorkerDelay, "workerDelay", 100*time.Millisecond,
		"sleep time between ballot stuffing")
	f.IntVar(&c.Workers, "workers", 1,
		"the number of concurrent ballot stuffers")
}

// Command returns the VOTR workload.
func Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "votr",
		Short: "a workload to demonstrate async, active-active replication",
	}
	cmd.AddCommand(commandInit(), commandRun())
	return cmd
}

// commandInit returns a Command wrapper around doInit.
func commandInit() *cobra.Command {
	cfg := &config{}
	cmd := &cobra.Command{
		Use:   "init",
		Short: "initialize the VOTR schema",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			// main.go provides the stopper.
			return doInit(stopper.From(cmd.Context()), cfg)
		},
	}
	cfg.Bind(cmd.Flags())
	return cmd
}

// doInit initializes the VOTR schema in the configured targets.
func doInit(ctx *stopper.Context, cfg *config) error {
	if len(cfg.Connect) < 2 {
		return errors.New("at least two connection strings are required")
	}

	schemas := make([]*schema, 0, len(cfg.Connect))

	for idx, conn := range cfg.Connect {
		sch, err := openSchema(ctx, cfg, region(idx))
		if err != nil {
			return errors.Wrapf(err, "could not connect to region %d at %s", idx, conn)
		}
		schemas = append(schemas, sch)
	}

	for _, sch := range schemas {
		if err := sch.create(ctx); err != nil {
			return errors.Wrapf(err, "could not create VOTR schema in %s", sch.region)
		}
	}

	return nil
}

// commandRun returns a Command wrapper around doRun.
func commandRun() *cobra.Command {
	cfg := &config{}
	cmd := &cobra.Command{
		Use:   "run",
		Short: "run the VOTR workload",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			// main.go provides the stopper.
			return doRun(stopper.From(cmd.Context()), cfg)
		},
	}
	cfg.Bind(cmd.Flags())
	return cmd
}

// doRun is the main votr demo.
func doRun(ctx *stopper.Context, cfg *config) error {
	if len(cfg.Connect) < 2 {
		return errors.New("at least two connection strings are required")
	}

	// Allow scripted shutdown, also used by test code.
	if cfg.StopAfter > 0 {
		ctx.Go(func() error {
			select {
			case <-ctx.Stopping():
			// Do nothing.
			case <-time.After(cfg.StopAfter):
				ctx.Stop(cfg.DrainDelay)
			}
			return nil
		})
	}

	// Run the requested number of workers.
	log.Infof("inserting with %d workers across %d candidates", cfg.Workers, cfg.Candidates)
	if cfg.WorkerDelay > 0 {
		log.Infof("theoretical max regional ballots per reporting interval: %d",
			int64(cfg.Workers)*int64(cfg.BallotBatch)*
				cfg.ReportInterval.Nanoseconds()/cfg.WorkerDelay.Nanoseconds())
		log.Info("low performance may indicate contention due to too few candidates")
	}

	// We want to run the servers with their own lifecycle in
	// order to allow in-flight mutations a chance to drain.
	svrStopper := stopper.WithContext(context.Background())
	defer svrStopper.Stop(0)

	// Allow a limit on the number of ballots to be set.
	if cfg.MaxBallots > 0 {
		cfg.remaining.Store(int64(cfg.MaxBallots))
		ctx.Go(func() error {
			// Wait for all ballots to be inserted.
			for cfg.remaining.Load() > 0 {
				select {
				case <-ctx.Stopping():
					return nil
				case <-time.After(time.Second):
				}
			}
			log.Infof("hit maximum number of ballots, waiting for replication")

			// Loop until we see the right number of ballots in the
			// target databases.
		top:
			for {
				select {
				case <-ctx.Stopping():
					return nil
				case <-time.After(time.Second):
				}

			schema:
				for _, s := range cfg.schemas {
					total, err := s.sumTotal(ctx)
					if err != nil {
						return err
					}
					if total < cfg.MaxBallots {
						// Waiting for replication.
						continue top
					} else if total == cfg.MaxBallots {
						// Success!
						continue schema
					} else {
						// Over-replication or other error.
						return errors.Errorf(
							"region %s had %d ballots, but expecting at most %d",
							s.region, total, cfg.MaxBallots)
					}
				}

				log.Infof("%d ballots inserted in all regions", cfg.MaxBallots)
				// Fast stop, since replication is done.
				ctx.Stop(10 * time.Millisecond)
				svrStopper.Stop(10 * time.Millisecond)
				return nil
			}
		})
	} else {
		cfg.remaining.Store(math.MaxInt64)
	}

	schemas := make([]*schema, 0, len(cfg.Connect))
	for idx := range cfg.Connect {
		sch, err := worker(ctx, cfg, region(idx))
		if err != nil {
			return err
		}
		schemas = append(schemas, sch)
	}
	cfg.schemas = schemas // Make available for post-hoc testing.

	// Create a connection between the hub and each of the
	// spokes. We could offer additional topologies, such as a
	// uni- or bi-directional ring or a fully-connected graph.
	hubSch := schemas[0]
	schemas = schemas[1:]

	for idx, leafSch := range schemas {
		toLeaf, err := startServer(svrStopper, cfg, hubSch, leafSch)
		if err != nil {
			return errors.Wrapf(err, "starting server %s -> %s", hubSch.region, leafSch.region)
		}

		// Each leaf needs its own server that will write
		// updates to the hub until such time as cdc-sink can
		// support multiple different changefeeds writing to the
		// same destination schema. See
		// https://github.com/cockroachdb/cockroach/issues/112880
		// for a way that this might become trivially easy.
		toHub, err := startServer(svrStopper, cfg, leafSch, hubSch)
		if err != nil {
			return errors.Wrapf(err, "starting server %s -> %s", leafSch.region, hubSch.region)
		}

		var toLeafJob int64
		if toLeafJob, err = createFeed(ctx, hubSch, toLeaf); err != nil {
			return errors.Wrapf(err, "feed %s -> %s", hubSch.region, toLeaf)
		}
		svrStopper.Defer(func() {
			if err := cancelFeed(stopper.Background(), hubSch, toLeafJob); err != nil {
				log.WithError(err).Warnf("could not cancel changefeed job %d in %d",
					toLeafJob, hubSch.region)
			}
		})

		var toHubJob int64
		if toHubJob, err = createFeed(ctx, schemas[idx], toHub); err != nil {
			return errors.Wrapf(err, "feed %s -> %s", hubSch.region, toHub)
		}
		svrStopper.Defer(func() {
			if err := cancelFeed(stopper.Background(), schemas[idx], toHubJob); err != nil {
				log.WithError(err).Warnf("could not cancel changefeed job %d in %d",
					toHubJob, schemas[idx].region)
			}
		})
	}

	// Wait for the workers to be shut down.
	if workerErr := ctx.Wait(); workerErr != nil {
		return workerErr
	}

	// This won't execute if the servers were already stopped by
	// reaching the maximum number of ballots.
	svrStopper.Go(func() error {
		log.Infof("workload stopped, pausing for %s to drain", cfg.DrainDelay)
		time.Sleep(cfg.DrainDelay)
		svrStopper.Stop(5 * time.Second)
		return nil
	})
	return svrStopper.Wait()
}

// cancelFeed cancels the changefeed job.
func cancelFeed(ctx *stopper.Context, in *schema, job int64) error {
	return retry.Retry(ctx, func(ctx context.Context) error {
		// We need to open a one-shot connection since the worker pool
		// already be closed at this point.
		conn, err := pgx.Connect(ctx, in.db.ConnectionString)
		if err != nil {
			return errors.Wrapf(err, "could not connect to %d to cancel job %d", in.region, job)
		}
		defer func() { _ = conn.Close(ctx) }()
		_, err = conn.Exec(ctx, "CANCEL JOB $1", job)
		if err == nil {
			log.Infof("cleaned up changefeed %d in %d", job, in.region)
		}
		return err
	})
}

// createFeed creates a changefeed from the given source to the given
// server. It returns the job id of the changefeed.
func createFeed(ctx *stopper.Context, from *schema, to *url.URL) (int64, error) {
	// Set the cluster settings once, if we need to.
	var enabled bool
	if err := retry.Retry(ctx, func(ctx context.Context) error {
		return from.db.QueryRowContext(ctx, "SHOW CLUSTER SETTING kv.rangefeed.enabled").Scan(&enabled)
	}); err != nil {
		return 0, errors.Wrap(err, "could not check cluster setting")
	}
	if !enabled {
		if err := retry.Retry(ctx, func(ctx context.Context) error {
			_, err := from.db.ExecContext(ctx, "SET CLUSTER SETTING kv.rangefeed.enabled = true")
			return errors.Wrapf(err, "%s: could not enable rangefeeds", from.region)
		}); err != nil {
			return 0, err
		}
	}

	var hasLicense bool
	if err := from.db.QueryRowContext(ctx,
		"SELECT (SELECT * FROM [SHOW CLUSTER SETTING enterprise.license]) <> ''",
	).Scan(&hasLicense); err != nil {
		return 0, errors.Wrapf(err, "%s: could not query for presense of license", from.region)
	}
	if !hasLicense {
		lic, hasLic := os.LookupEnv("COCKROACH_DEV_LICENSE")
		org, hasOrg := os.LookupEnv("COCKROACH_DEV_ORGANIZATION")
		if hasLic && hasOrg {
			if err := retry.Retry(ctx, func(ctx context.Context) error {
				_, err := from.db.ExecContext(ctx, "SET CLUSTER SETTING enterprise.license = $1", lic)
				return errors.Wrapf(err, "%s: could not set cluster license", from.region)
			}); err != nil {
				return 0, err
			}

			if err := retry.Retry(ctx, func(ctx context.Context) error {
				_, err := from.db.ExecContext(ctx, "SET CLUSTER SETTING cluster.organization = $1", org)
				return errors.Wrapf(err, "%s: could not set cluster organization", from.region)
			}); err != nil {
				return 0, err
			}
		} else {
			return 0, errors.New("changefeeds require an enterprise license; use 'cockroach demo' command")
		}
	}

	q := fmt.Sprintf(`CREATE CHANGEFEED FOR TABLE %s, %s, %s INTO '%s'
WITH diff, updated, resolved='1s', min_checkpoint_frequency='1s',
webhook_sink_config='{"Flush":{"Messages":1000,"Frequency":"1s"}}'`,
		from.ballots, from.candidates, from.totals, to.String(),
	)

	var job int64
	if err := retry.Retry(ctx, func(ctx context.Context) error {
		err := from.db.QueryRowContext(ctx, q).Scan(&job)
		return errors.Wrapf(err, "%s: could not create changefeed", from.region)
	}); err != nil {
		return 0, err
	}

	log.Infof("created feed from %s into %s", from.region, to)
	return job, nil
}

func openSchema(ctx *stopper.Context, cfg *config, r region) (*schema, error) {
	conn := cfg.Connect[r]

	pool, err := stdpool.OpenPgxAsTarget(ctx, conn,
		stdpool.WithConnectionLifetime(5*time.Minute),
		stdpool.WithPoolSize(cfg.Workers+1),
		stdpool.WithTransactionTimeout(time.Minute),
	)
	if err != nil {
		return nil, errors.Wrapf(err, "could not connect to %s database", r)
	}

	return newSchema((*types.SourcePool)(pool), cfg.Enclosing, r), nil
}

//go:embed script/*
var scriptFS embed.FS

// startServer runs an instance of cdc-sink which will feed into
// the given destination. It returns the base URL for delivering
// messages to the sink.
func startServer(ctx *stopper.Context, cfg *config, src, dest *schema) (*url.URL, error) {
	targetConn := cfg.Connect[dest.region]

	stagingName := ident.New(fmt.Sprintf("cdc_sink_%d_%s_%s", os.Getpid(), src.region, dest.region))
	stagingSchema := ident.MustSchema(dest.enclosing, stagingName)

	if _, err := dest.db.ExecContext(ctx, fmt.Sprintf(
		`CREATE SCHEMA IF NOT EXISTS %s`, stagingSchema)); err != nil {
		return nil, errors.Wrap(err, dest.region.String())
	}

	srvConfig := &server.Config{
		CDC: cdc.Config{
			BaseConfig: logical.BaseConfig{
				BackfillWindow:     0,
				ForeignKeysEnabled: true,
				RetryDelay:         10 * time.Second,
				ScriptConfig: script.Config{
					FS: &subfs.SubstitutingFS{
						FS: scriptFS,
						Replacer: strings.NewReplacer(
							"DESTINATION_DB", fmt.Sprintf(`"%s_%s"`,
								cfg.Enclosing.Raw(), dest.region.String()),
							"DESTINATION_INDEX", dest.region.String(),
							"SOURCE_INDEX", src.region.String(),
						),
					},
					MainPath: "/script/votr.ts",
				},
				StagingConn:   targetConn,
				StagingSchema: stagingSchema,
				TargetConn:    targetConn,
			},
			FlushEveryTimestamp: true, //  Needed for delta behavior.
			MetaTableName:       ident.New("resolved_timestamps"),
			RetireOffset:        24 * time.Hour, // For debugging.
		},
		BindAddr:           fmt.Sprintf("%s:0", cfg.SinkHost),
		DisableAuth:        true,
		GenerateSelfSigned: true,
	}
	if err := srvConfig.Preflight(); err != nil {
		return nil, errors.Wrap(err, dest.region.String())
	}
	srv, err := server.NewServer(ctx, srvConfig)
	if err != nil {
		return nil, errors.Wrap(err, dest.region.String())
	}
	_, port, err := net.SplitHostPort(srv.GetAddr().String())
	if err != nil {
		return nil, errors.Wrap(err, dest.region.String())
	}

	sink := &url.URL{
		Scheme:   "webhook-https",
		Host:     fmt.Sprintf("%s:%s", cfg.SinkHost, port),
		Path:     ident.Join(dest.candidates.Schema(), ident.Raw, '/'),
		RawQuery: "insecure_tls_skip_verify=true",
	}

	return sink, nil
}

// worker will launch a number of goroutines into the context to insert
// ballots and to verify the consistency of the dataset.
func worker(ctx *stopper.Context, cfg *config, r region) (*schema, error) {
	sch, err := openSchema(ctx, cfg, r)
	if err != nil {
		return nil, err
	}

	if err := sch.ensureCandidates(ctx, cfg.Candidates); err != nil {
		return nil, errors.Wrapf(err, "%s: could not create candidate entries", r)
	}

	warnings, err := sch.validate(ctx, false)
	if err != nil {
		return nil, errors.Wrapf(err, "%s: could not perform initial validation", r)
	}
	if len(warnings) > 0 {
		log.WithField(
			"inconsistent", warnings,
		).Warnf("%s: workload starting from inconsistent state", r)
		if cfg.ErrInconsistent {
			return nil, errors.Errorf("exiting due to inconsistency in %s", r)
		}
	}

	// Don't run the workload on the hub if we're in a hub-and-spoke
	// model. The ON UPDATE clause in the table definitions isn't able
	// to refer to the existing value in the column. Thus, we cannot
	// transparently patch the vector clock without making the workload
	// itself aware of the vector clock column.
	//
	// In a two-region setup, we only need to be able to prevent a
	// mutation from cycling between the regions, so the vector clock is
	// an over-complication.
	if len(cfg.Connect) == 2 || r > 0 {
		ballotsToReport := &atomic.Int64{}
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
					}

					batchSize := int64(cfg.BallotBatch)
					remaining := cfg.remaining.Add(-batchSize) + batchSize
					if remaining <= 0 {
						return nil
					}
					if remaining < batchSize {
						batchSize = remaining
					}

					if err := sch.doStuff(ctx, int(batchSize)); err != nil {
						return errors.Wrap(err, "could not stuff ballots")
					}
					ballotsToReport.Add(batchSize)
				}
			})
		}

		// Print status.
		ctx.Go(func() error {
			for {
				select {
				case <-time.After(cfg.ReportInterval):
					log.Infof("%s: inserted %d ballots", r, ballotsToReport.Swap(0))
				case <-ctx.Stopping():
					return nil
				}
			}
		})
	}

	// Start a background validation loop.
	ctx.Go(func() error {
		for {
			select {
			case <-time.After(cfg.ValidationDelay):
			case <-ctx.Stopping():
				return nil
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
			if cfg.ErrInconsistent {
				return errors.Errorf("exiting due to inconsistency in %s", r)
			}
		}
	})

	return sch, nil
}
