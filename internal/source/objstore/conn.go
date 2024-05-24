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
	"context"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	backoff "github.com/cenkalti/backoff/v4"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/source/objstore/bucket"
	"github.com/cockroachdb/replicator/internal/source/objstore/eventproc"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/cdcjson"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

const (
	resolvedSuffix = ".RESOLVED"
)

// Conn provides the connection to the object store.
type Conn struct {
	// Access to cloud storage.
	bucket bucket.Bucket
	// The connector configuration.
	config *Config
	// Delivers mutation to the target database.
	conveyor Conveyor
	// Ensures that only one replicator instances acts on file.
	leases types.Leases
	// Parser for ndjson files.
	parser *cdcjson.NDJsonParser
	// event processor
	processor eventproc.Processor
	// access to staging database
	stagingPool *types.StagingPool
	// persistent state
	state state
}

// Start the replication loop.
func (c *Conn) Start(ctx *stopper.Context) (err error) {
	// Ensure that only one replicator process is scanning for files in
	// the object store.
	ctx.Go(func(ctx *stopper.Context) error {
		c.leases.Singleton(ctx,
			fmt.Sprintf("%s.objstore.scan", c.config.bucketName),
			func(ctx context.Context) error {
				if err := c.apply(stopper.WithContext(ctx), ""); err != nil && err != stopper.ErrStopped {
					log.WithField("bucket", c.config.bucketName).
						Errorf("object store apply failed with %v", err)
					return err
				}
				log.WithField("bucket", c.config.bucketName).
					Info("object store shutting down")
				return types.ErrCancelSingleton
			})
		return nil
	})
	return nil
}

// resolvedRange identifies a set of entries in the object store between
// two consecutive resolved timestamps. It assumes a lexicographic
// filename order.
type resolvedRange struct {
	count    int // estimate of the number of entries.
	from, to string
}

// Sink cloud storage guarantees:
// The resolved timestamp files are named `<timestamp>.RESOLVED`. This is
// carefully done so that we can offer the following external guarantee: At any
// given time, if the files are iterated in lexicographic filename order,
// then encountering any filename containing `RESOLVED` means that everything
// before it is finalized (and thus can be ingested into some other system and
// deleted, included in hive queries, etc). A typical user of cloudStorageSink
// would periodically do exactly this.
// (see https://github.com/asg0451/cockroach/blob/75fb68babf8e4392eee8fe4dc150299c22dc7962/pkg/ccl/changefeedccl/sink_cloudstorage.go#L112)

// apply walks the files available in the bucket and process them.
func (c *Conn) apply(ctx *stopper.Context, dir string) error {
	last, err := c.state.getLast(ctx, c.stagingPool)
	if err != nil {
		return err
	}
	resolvedRanges := make(chan (*resolvedRange))
	// Start a go routine to find resolved timestamps
	ctx.Go(func(ctx *stopper.Context) error {
		defer close(resolvedRanges)
		for {
			r, err := c.findResolved(ctx, dir, last)
			if err != nil {
				return err
			}
			select {
			case resolvedRanges <- r:
			case <-ctx.Stopping():
				return nil
			}
			last = r.to
		}
	})
	// Start a go routine to process files between two resolved timestamps.
	ctx.Go(func(ctx *stopper.Context) error {
		for resolvedRange := range resolvedRanges {
			start := time.Now()
			err := c.applyRange(ctx, dir, resolvedRange)
			applyDuration.WithLabelValues(c.config.bucketName).
				Observe(float64(time.Since(start).Seconds()))
			if err != nil {
				return err
			}
		}
		return nil
	})
	return ctx.Wait()
}

// applyRange fetches all the files between two resolved timestamps and
// forwards them to a processor.
func (c *Conn) applyRange(ctx *stopper.Context, dir string, res *resolvedRange) error {
	if res == nil {
		// This shouldn't happen.
		log.WithField("bucket", c.config.bucketName).Warnf("got an empty range")
		return nil
	}
	fileBatch := make([]string, 0, res.count)
	options := &bucket.WalkOptions{
		StartAfter: res.from,
		Limit:      bucket.NoLimit,
		Recursive:  true,
	}
	log.WithField("bucket", c.config.bucketName).
		Debugf("applyRange %q %q", res.from, res.to)
	operation := func() error {
		return c.bucket.Walk(ctx, dir, options,
			func(ctx *stopper.Context, file string) error {
				file = path.Join(c.config.bucketName, file)
				log.WithField("bucket", c.config.bucketName).
					Tracef("processing %q", file)
				compare := strings.Compare(file, res.to)
				switch {
				case compare == 0:
					// We found the upper bound resolved timestamp.
					// We can process all the files we collected.
					var buff io.ReadCloser
					open := func() error {
						var err error
						buff, err = c.bucket.Open(ctx, file)
						return err
					}
					err := c.retry(open, "open file")
					if err != nil {
						return errors.Wrapf(err, "failed to retrieve %q", file)
					}
					defer buff.Close()
					time, err := c.parser.Resolved(buff)
					if err != nil {
						return err
					}
					log.WithField("bucket", c.config.bucketName).
						Tracef("processing batch at %q; size %d", time, len(fileBatch))
					if err := c.processBatch(ctx, time, fileBatch); err != nil {
						return err
					}
					// If all the the files have been processed, we can persist
					// the resolved timestamp into the memo table.
					if err := c.state.setLast(ctx, c.stagingPool, file); err != nil {
						return err
					}
					// We can skip the remaining entries, since they are outside
					// the resolve timestamps range.
					return bucket.ErrSkipAll
				case compare < 0:
					// Appending the file to the batch we need to process.
					// TODO (silvano): we might want to consider to flush
					// process the batch periodically.
					fileBatch = append(fileBatch, file)
				default:
					// This shouldn't really happen, unless the resolved timestamp
					// has been deleted.
					return errors.Errorf("resolved %q timestamp not found", res.to)
				}
				return nil
			})
	}
	return c.retry(operation, "apply range")
}

// findResolved finds the next range of entries to processes.
// Currently, it's between two consecutive resolved timestamps.
func (c *Conn) findResolved(
	ctx *stopper.Context, dir string, lowerBound string,
) (*resolvedRange, error) {
	ticker := time.NewTicker(c.config.FetchDelay)
	var upperBound string
	// Number of entries between two resolved timestamps.
	count := 0
	for upperBound == "" {
		start := time.Now()
		options := &bucket.WalkOptions{
			StartAfter: lowerBound,
			Limit:      bucket.NoLimit,
			Recursive:  true,
		}
		operation := func() error {
			return c.bucket.Walk(ctx, dir, options,
				func(ctx *stopper.Context, file string) error {
					log.WithField("bucket", c.config.bucketName).
						Tracef("processing %s", file)
					file = path.Join(c.config.bucketName, file)

					if strings.HasSuffix(file, resolvedSuffix) {
						batchSize.WithLabelValues(c.config.bucketName).Set(float64(count))
						if count > 0 {
							// We found a range with mutations, we will stop
							// the walk
							upperBound = file
							return bucket.ErrSkipAll
						}
						log.WithField("bucket", c.config.bucketName).
							Debugf("no transactions between %s and %s", lowerBound, file)
						// If we get here, there is nothing interesting in
						// between resolved timestamps.
						// Resetting the lower bound of the range we need to process.
						lowerBound = file

						// TODO (silvano): we might be able to persist the
						// last resolved timestamp to memo, however we have
						// to make sure that all the concurrent processors
						// are done. For busy systems this shouldn't matter,
						// as it is unlikely that we don't have transactions
						// between two resolved timestamps.

						return nil
					}
					count++
					select {
					case <-ctx.Stopping():
						return bucket.ErrSkipAll
					default:
					}
					return nil
				})
		}
		err := c.retry(operation, "find resolved")
		if err != nil {
			log.WithField("bucket", c.config.bucketName).
				Errorf("find resolved %v", err)
			return nil, err
		}
		fetchResolvedDuration.WithLabelValues(c.config.bucketName).
			Observe(float64(time.Since(start).Seconds()))
		select {
		case <-ctx.Stopping():
			return nil, stopper.ErrStopped
		case <-ticker.C:
		}
	}
	return &resolvedRange{
		from:  lowerBound,
		to:    upperBound,
		count: count,
	}, nil
}

// processBatch receives a list of files and dispatches a processor for each file on
// a separate go routines.
func (c *Conn) processBatch(ctx *stopper.Context, resolved hlc.Time, files []string) error {
	if c.config.Workers < 1 {
		return errors.Errorf("invalid number of workers %d", c.config.Workers)
	}
	var g errgroup.Group
	g.SetLimit(c.config.Workers)
	for _, file := range files {
		file := file
		operation := func() error {
			return c.processor.Process(ctx, file)
		}
		g.Go(func() error {
			start := time.Now()
			err := c.retry(operation, fmt.Sprintf("processing %s", file))
			processDuration.WithLabelValues(c.config.bucketName).
				Observe(float64(time.Since(start).Seconds()))
			return err
		})
	}
	err := g.Wait()
	if err != nil {
		return err
	}
	return c.conveyor.Advance(ctx, ident.New(c.config.bucketName), resolved)
}

// retry calls the operation and retries if we get a transient error.
// TODO (silvano) - Revisit, simple for now.
func (c *Conn) retry(operation backoff.Operation, label string) error {
	retryOp := func() error {
		err := operation()
		if !errors.Is(err, bucket.ErrTransient) {
			return backoff.Permanent(err)
		}
		return err
	}
	notify := func(err error, delay time.Duration) {
		retryCount.WithLabelValues(label).Inc()
		log.WithError(err).Errorf("%s failed after %s; retrying", label, delay)
	}
	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.MaxElapsedTime = c.config.RetryMaxTime
	expBackoff.InitialInterval = c.config.RetryInitialInterval
	return backoff.RetryNotify(retryOp, expBackoff, notify)
}
