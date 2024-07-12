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
		log.Infof("Acquiring lease %s", c.config.identifier)
		c.leases.Singleton(ctx,
			c.config.identifier,
			func(ctx context.Context) error {
				if err := c.apply(stopper.WithContext(ctx), c.config.prefix); err != nil && err != stopper.ErrStopped {
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
// (see https://github.com/cockroachdb/cockroach/blob/8a43fac15a431b6a85a33da292cbd3ecb662f169/pkg/ccl/changefeedccl/sink_cloudstorage.go#L112)

// apply walks the files available in the bucket and process them.
func (c *Conn) apply(ctx *stopper.Context, dir string) error {
	last, err := c.state.getLast(ctx, c.stagingPool)
	if err != nil {
		return err
	}
	minResolved, ok, err := c.findStartingResolved(ctx, dir)
	if err != nil {
		return err
	}
	if ok && last < minResolved {
		last = minResolved
	}
	resolvedRanges := make(chan (*resolvedRange))

	// Start a go routine to find resolved timestamps
	ctx.Go(func(ctx *stopper.Context) error {
		defer close(resolvedRanges)
		for {
			r, err := c.findResolved(ctx, dir, last, false)
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
	// Check if the resolved timestamps are within the min/max timestamp.
	validRange, err := c.checkValidRange(ctx, res)
	if err != nil {
		return err
	}
	// if not, we just update the state
	if !validRange {
		log.WithField("bucket", c.config.bucketName).
			Debugf("skipping %s %s", res.from, res.to)
		return c.state.setLast(ctx, c.stagingPool, res.to)
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
					time, err := c.getResolvedTimestamp(ctx, file)
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

// checkValidRange verifies that the resolved timestamps in the
// given range overlaps with the time range specified in the
// configuration as [MinTimestamp - MaxTimestamp).
func (c *Conn) checkValidRange(ctx *stopper.Context, res *resolvedRange) (bool, error) {
	var lowerBound hlc.Time
	if res.from != "" {
		var err error
		lowerBound, err = c.getResolvedTimestamp(ctx, res.from)
		if err != nil {
			return false, err
		}
	}
	upperBound, err := c.getResolvedTimestamp(ctx, res.to)
	if err != nil {
		return false, err
	}
	return c.config.timeRange.Contains(lowerBound) ||
		c.config.timeRange.Contains(upperBound), nil
}

// getResolvedTimestamp retrieves a resolved timestamp message located at the given path.
func (c *Conn) getResolvedTimestamp(ctx *stopper.Context, file string) (hlc.Time, error) {
	var buff io.ReadCloser
	open := func() error {
		var err error
		buff, err = c.bucket.Open(ctx, file)
		return err
	}
	err := c.retry(open, "open file")
	if err != nil {
		return hlc.Zero(), errors.Wrapf(err, "failed to retrieve %q", file)
	}
	defer buff.Close()
	res, err := c.parser.Resolved(buff)
	if err != nil {
		return hlc.Zero(), errors.Wrapf(err, "failed to parse timestamp in file %q", file)
	}
	return res, nil
}

// filePrefix returns a path within the connection's bucket based on the configured
// partition file format.
func (c *Conn) filePrefix(dir string, timestamp time.Time) (string, error) {
	switch c.config.PartitionFormat {
	case Daily:
		return path.Join(c.config.bucketName, dir,
			timestamp.Format("2006-01-02"),
			timestamp.Format("20060102150405")), nil
	case Hourly:
		return path.Join(c.config.bucketName, dir,
			timestamp.Format("2006-01-02"),
			timestamp.Format("15"),
			timestamp.Format("20060102150405")), nil
	case Flat:
		return path.Join(c.config.bucketName, dir,
			timestamp.Format("20060102150405")), nil
	default:
		return "",
			errors.Errorf("invalid partition format %s", c.config.PartitionFormat)
	}
}

// findStartingResolved discovers a file that has a resolved timestamp
// before the user supplied minimum timestamp.
// It returns a comma-ok if the file was found.
func (c *Conn) findStartingResolved(ctx *stopper.Context, dir string) (string, bool, error) {
	if hlc.Compare(c.config.MinTimestamp, hlc.Zero()) == 0 {
		return "", false, nil
	}
	earliestResolved, err := c.findResolved(ctx, dir, "", true)
	if err != nil {
		return "", false, err
	}
	// nothing found in the bucket.
	if earliestResolved.to == "" {
		return "", false, nil
	}
	min := time.Unix(0, c.config.timeRange.Min().Nanos()).UTC()
	for {
		// we'll build the filename
		from, err := c.filePrefix(dir, min)
		if err != nil {
			log.WithError(err).
				WithField("bucket", c.config.bucketName).
				Errorf("cannot determine prefix from %q", min)
			return "", false, err
		}
		rng, err := c.findResolved(ctx, dir, from, true)
		if err != nil {
			log.WithError(err).
				WithField("bucket", c.config.bucketName).
				Errorf("cannot find resolved timestamp file starting at %q", from)
			return "", false, err
		}
		// found the earliest, there is nothing before it.
		if earliestResolved.to == rng.to {
			return rng.to, true, nil
		}
		resolved, err := c.getResolvedTimestamp(ctx, rng.to)
		if err != nil {
			log.WithField("bucket", c.config.bucketName).
				Errorf("cannot get resolved timestamp %q", err)
			return "", false, err
		}
		// we have to start to scan just before the given min timestamp
		if hlc.Compare(resolved, c.config.timeRange.Min()) <= 0 {
			return rng.to, true, nil
		}
		// go back and try again.
		min = min.Add(-c.config.ResolvedInterval)
	}
}

// findResolved discovers ranges of files between two consecutive resolved timestamps.
// It returns also ranges with no transactions if returnEmpty is true.
func (c *Conn) findResolved(
	ctx *stopper.Context, dir string, lowerBound string, returnEmpty bool,
) (*resolvedRange, error) {
	log.WithField("bucket", c.config.bucketName).
		Tracef("findResolved %s", lowerBound)
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
						batchSize.WithLabelValues(c.config.bucketName).Observe(float64(count))
						if count > 0 || returnEmpty {
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
					if ctx.IsStopping() {
						return bucket.ErrSkipAll
					}
					return nil
				})
		}
		err := c.retry(operation, "find resolved")
		if err != nil {
			log.WithField("bucket", c.config.bucketName).WithError(err).
				Errorf("find resolved failed")
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
			return c.processor.Process(ctx, file, func(mut types.Mutation) bool {
				return c.config.timeRange.Contains(mut.Time)
			})
		}
		g.Go(func() error {
			start := time.Now()
			err := c.retry(operation, fmt.Sprintf("processing %s", file))
			processDuration.WithLabelValues(c.config.bucketName).
				Observe(float64(time.Since(start).Seconds()))
			return errors.Wrapf(err, "process %q failed", file)
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
