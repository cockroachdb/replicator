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
	"math"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stdpool"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type config struct {
	BallotBatch     int
	Candidates      int
	Connect         string
	Enclosing       ident.Schema
	ReportInterval  time.Duration
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
	f.StringVar(&c.Connect, "connect",
		"postgresql://root@localhost:26257/?sslmode=disable",
		"the CockroachDB connection string")
	c.Enclosing = ident.MustSchema(ident.New("votr"), ident.Public)
	f.Var(ident.NewSchemaFlag(&c.Enclosing), "schema",
		"the enclosing database schema")
	f.DurationVar(&c.ReportInterval, "reportiAfter", 5*time.Second,
		"report number of ballots inserted")
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
	cmd.AddCommand(commandInit(), commandWorker())
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
			pool, cancel, err := stdpool.OpenPgxAsTarget(ctx, cfg.Connect)
			if err != nil {
				return errors.Wrap(err, "could not connect to target database")
			}
			defer cancel()

			sch := newSchema(pool.DB, cfg.Enclosing)
			if err := sch.create(ctx); err != nil {
				return errors.Wrap(err, "could not create VOTR schema")
			}
			return nil
		},
	}
	cfg.Bind(cmd.Flags())
	return cmd
}

func commandWorker() *cobra.Command {
	cfg := &config{}
	cmd := &cobra.Command{
		Use:   "run",
		Short: "run the VOTR workload",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			pool, cancel, err := stdpool.OpenPgxAsTarget(cmd.Context(), cfg.Connect,
				stdpool.WithConnectionLifetime(5*time.Minute),
				stdpool.WithPoolSize(cfg.Workers+1),
				stdpool.WithTransactionTimeout(time.Minute),
			)
			if err != nil {
				return errors.Wrap(err, "could not connect to target database")
			}
			defer cancel()

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

			// Ensure tables and the (deterministic) candidate rows.
			sch := newSchema(pool.DB, cfg.Enclosing)
			if err := sch.ensureCandidates(ctx, cfg.Candidates); err != nil {
				return errors.Wrap(err, "could not create candidate entries")
			}

			warnings, err := sch.validate(ctx, false)
			if err != nil {
				return errors.Wrap(err, "could not perform initial validation")
			}
			if len(warnings) > 0 {
				log.WithField("inconsistent", warnings).Warn("workload starting from inconsistent state")
			}

			// Run the requested number of workers.
			count := &atomic.Int64{}
			for i := 0; i < cfg.Workers; i++ {
				ctx.Go(func() error { return worker(ctx, cfg, sch, count) })
			}
			log.Infof("inserting with %d workers across %d candidates",
				cfg.Workers, len(sch.candidateIds))
			if cfg.WorkerDelay > 0 {
				log.Infof("theoretical max ballots per reporting interval: %d",
					int64(cfg.Workers)*int64(cfg.BallotBatch)*
						cfg.ReportInterval.Nanoseconds()/cfg.WorkerDelay.Nanoseconds())
				log.Info("low performance may indicate contention due to too few candidates")
			}

			// Print status.
			ctx.Go(func() error {
				for {
					select {
					case <-time.After(cfg.ReportInterval):
						log.Infof("inserted %d ballots", count.Swap(0))
					case <-ctx.Stopping():
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			})

			// Start a background validation loop.
			ctx.Go(func() error { return validator(ctx, cfg, sch) })

			return ctx.Wait()
		},
	}
	cfg.Bind(cmd.Flags())
	return cmd
}

func validator(ctx *stopper.Context, cfg *config, sch *schema) error {
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
			log.Info("workload is consistent")
			continue
		}
		log.WithField("inconsistent", warnings).Warn("workload in inconsistent state")
	}
}

func worker(ctx *stopper.Context, cfg *config, sch *schema, count *atomic.Int64) error {
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
		count.Add(int64(cfg.BallotBatch))
	}
}
