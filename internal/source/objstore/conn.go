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

// Conn provides the connection to the object store.
type Conn struct {
	// Access to cloud storage.
	bucket bucket.Reader
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

// resolvedRange identifies a set of entries in the bucket between
// two consecutive resolved timestamps.
type resolvedRange struct {
	count    int // estimate of the number of entries.
	from, to string
}

// Start the replication loop.
func (c *Conn) Start(ctx *stopper.Context) (err error) {
	// We only need one replicator process to snapshot file names from the bucket
	ctx.Go(func(ctx *stopper.Context) error {
		c.leases.Singleton(ctx,
			fmt.Sprintf("%s.snapshot", c.config.bucketName),
			func(ctx context.Context) error {
				if err := c.apply(stopper.WithContext(ctx), ""); err != nil {
					log.WithField("bucket", c.config.bucketName).
						Errorf("apply failed %s", err)
					return err
				}
				return types.ErrCancelSingleton
			})
		return nil
	})
	return nil
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
			// r is nil, with no error if we stopped the scan.
			if r == nil {
				return nil
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
			err := c.applyRange(ctx, dir, resolvedRange)
			if err != nil {
				return err
			}
		}
		return nil
	})
	<-ctx.Stopping()
	return nil
}

// applyRange fetches all the files between two resolved timestamps and
// forwards them to a processor.
func (c *Conn) applyRange(ctx *stopper.Context, dir string, res *resolvedRange) error {
	if res == nil {
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
				buff, err := c.bucket.Open(ctx, file)
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
				fileBatch = append(fileBatch, file)
			case compare > 0:
				// This shouldn't really happen, unless the resolved timestamp
				// has been deleted.
				return errors.Errorf("resolved %q timestamp not found", res.to)
			}
			return nil
		})
}

// findResolved finds the next range of entries to processes.
// Currently, it's between two consecutive resolved timestamps.
func (c *Conn) findResolved(ctx *stopper.Context, dir string, from string) (*resolvedRange, error) {
	ticker := time.NewTicker(c.config.FetchDelay)
	found := false
	last := from
	// Number of entries between two resolved timestamps.
	count := 0
	for !found {
		options := &bucket.WalkOptions{
			StartAfter: last,
			Limit:      bucket.NoLimit,
			Recursive:  true,
		}
		err := c.bucket.Walk(ctx, dir, options,
			func(ctx *stopper.Context, file string) error {
				log.WithField("bucket", c.config.bucketName).
					Tracef("processing %s", file)
				file = path.Join(c.config.bucketName, file)

				if strings.HasSuffix(file, ".RESOLVED") {
					// Since we found a resolved file, we will start the next
					// scan from this.
					last = file
					if count > 0 {
						// We found a range with mutations, we will stop
						// the walk
						found = true
						return bucket.ErrSkipAll
					}
					// If we get here, there is nothing interesting in
					// between resolved timestamps.

					// TODO (silvano): we might be able to persist the
					// last resolved timestamp to memo, however we have
					// to make sure that all the concurrent processors
					// are done. For busy systems this shouldn't matter,
					// as it is unlikely that we don't have transactions
					// between two resolved timestamps.

					// Resetting the lower bound of the range we need to process.
					from = file
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
		if err != nil {
			log.WithField("bucket", c.config.bucketName).
				Errorf("find resolved %v", err)
			return nil, err
		}
		select {
		case <-ctx.Stopping():
			return nil, nil
		case <-ticker.C:
		}
	}
	return &resolvedRange{
		from:  from,
		to:    last,
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
		op := func() error {
			err := c.processor.Process(ctx, file)
			if !errors.Is(err, bucket.ErrTransient) {
				return backoff.Permanent(err)
			}
			return err
		}
		g.Go(func() error {
			// TODO (silvano) - Revisit, simple for now.
			b := backoff.NewExponentialBackOff()
			b.MaxElapsedTime = 10 * time.Second
			b.InitialInterval = 100 * time.Millisecond
			return backoff.Retry(op, b)
		})
	}
	err := g.Wait()
	if err != nil {
		return err
	}
	return c.conveyor.Advance(ctx, ident.New(c.config.bucketName), resolved)
}
