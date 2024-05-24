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
	"io/fs"
	"math/big"
	"path"
	"slices"
	"testing"
	"testing/fstest"
	"time"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/sinktest/mocks"
	"github.com/cockroachdb/replicator/internal/source/objstore/providers/local"
	"github.com/cockroachdb/replicator/internal/staging/memo"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/cdcjson"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// Directory where to store the files in the in-memory filesystem
	baseDir = "mybucket"
	// How often we should throw a transient error.
	defaultProbTransientError = 0.005
	// How many files to write in the bucket
	// Use a prime number to guarantee the last file is a resolved timestamp.
	defaultUpperLimit = 2003
)

// templates
const (
	ndjsonContent    = `{"after": {"p": %[1]d}, "key": [%[1]d], "updated": "%[1]d.0000000000"}`
	ndjsonFileName   = `%09d.ndjson`
	resolvedContent  = `{"resolved":"%d.0000000000"}`
	resolvedFileName = `%09d.RESOLVED`
)

// TestApply verifies we can process all the resolved timestamps and ndjson files.
func TestApply(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stop := stopper.WithContext(ctx)
	r := require.New(t)
	a := assert.New(t)
	rootFS := make(fstest.MapFS)
	ranges, batches, err := generate(rootFS, baseDir, defaultUpperLimit)
	r.NoError(err)
	expected := make([]string, 0, defaultUpperLimit)
	for _, v := range batches {
		expected = append(expected, v...)
	}
	slices.Sort(expected)
	conn, collector, err := buildConn(rootFS)
	r.NoError(err)
	stop.Go(
		func(ctx *stopper.Context) error {
			return conn.apply(ctx, ".")
		})
	ticker := time.NewTicker(100 * time.Millisecond)
	// wait until we see all the files accumulated in the processor
	for {
		if len(collector.GetSorted()) >= len(expected) {
			stop.Stop(time.Second)
			break
		}
		select {
		case <-stop.Stopping():
			r.Fail("process has stopped")
		case <-ticker.C:
		}

	}
	a.Equal(expected, collector.GetSorted())
	lastTimestamp, err := conn.state.getLast(ctx, nil)
	a.NoError(err)
	a.Equal(lastTimestamp, ranges[len(ranges)-1].to)
}

// TestApplyRange verifies that we can process all the files between two
// consecutive resolved timestamps.
func TestApplyRange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stop := stopper.WithContext(ctx)
	r := require.New(t)
	a := assert.New(t)
	rootFS := make(fstest.MapFS)
	ranges, batches, err := generate(rootFS, baseDir, defaultUpperLimit)
	r.NoError(err)
	conn, _, err := buildConn(rootFS)
	r.NoError(err)
	for _, rg := range ranges {
		conveyor := &timestampTracker{}
		collector := mocks.NewCollector(defaultUpperLimit)
		conn.processor = mocks.NewChaosProcessor(collector, defaultProbTransientError)
		conn.conveyor = conveyor
		err := conn.applyRange(stop, ".", rg)
		r.NoError(err)
		a.Equal(batches[rg.from], collector.GetSorted())
		timestamp, err := readTimestamp(rootFS, rg.to)
		r.NoError(err)
		a.Equal(conveyor.timestamp, timestamp)
	}
}

// TestFindResolved verifies that we scan the object store, and find ranges
// of files between two consecutive resolved timestamps.
func TestFindResolved(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stop := stopper.WithContext(ctx)
	r := require.New(t)
	a := assert.New(t)
	rootFS := make(fstest.MapFS)
	ranges, _, err := generate(rootFS, baseDir, defaultUpperLimit)
	r.NoError(err)
	conn, _, err := buildConn(rootFS)
	r.NoError(err)

	from := ""
	for _, expected := range ranges {
		got, err := conn.findResolved(stop, ".", from)
		a.NoError(err)
		a.Equal(expected, got)
		from = got.to
	}
}

// timestampTracker is used in the tests to keep track of the latest timestamp.
// It is injected as a Conveyor.
type timestampTracker struct {
	bucketName string
	timestamp  hlc.Time
}

var _ Conveyor = &timestampTracker{}

// AcceptMultiBatch implements Conveyor.
func (m *timestampTracker) AcceptMultiBatch(
	context.Context, *types.MultiBatch, *types.AcceptOptions,
) error {
	panic("unimplemented")
}

// Advance implements Conveyor.
func (m *timestampTracker) Advance(_ context.Context, partition ident.Ident, ts hlc.Time) error {
	if partition.Raw() != m.bucketName {
		return errors.Errorf("invalid partition name %q; %q expected", partition, m.bucketName)
	}
	m.timestamp = ts
	return nil
}

// Ensure implements Conveyor.
func (m *timestampTracker) Ensure(context.Context, []ident.Ident) error {
	panic("unimplemented")
}

func buildConn(fs fs.FS) (*Conn, *mocks.Collector, error) {
	bucket, err := local.New(fs)
	if err != nil {
		return nil, nil, err
	}
	conveyor := &timestampTracker{}
	collector := mocks.NewCollector(defaultUpperLimit)
	return &Conn{
		bucket: mocks.NewChaosBucket(bucket, defaultProbTransientError),
		config: &Config{
			FetchDelay: time.Millisecond,
			Workers:    8,
		},
		conveyor:  conveyor,
		processor: mocks.NewChaosProcessor(collector, defaultProbTransientError),
		state: state{
			memo: &memo.Memory{},
			key:  "test",
		},
	}, collector, nil
}

// generate test files and stores them into a in memory filesystem. The
// file names are generated with a padded, sequential natural number
// starting from 0. If the number is a prime, then we generate a
// resolved timestamp file, otherwise we generate a ndjson file that
// contains one single event. The primeUpperBound must be prime
// to ensure that the last file is resolved timestamp.
func generate(
	fs fstest.MapFS, dir string, primeUpperBound int,
) ([]*resolvedRange, map[string][]string, error) {
	if !big.NewInt(int64(primeUpperBound)).ProbablyPrime(0) {
		return nil, nil, errors.Errorf("primeUpperBound must be prime. %d is not",
			primeUpperBound)
	}
	ranges := make([]*resolvedRange, 0)
	batches := make(map[string][]string, 0)
	current := &resolvedRange{}
	name := ""
	content := ""
	batches[current.from] = make([]string, 0)
	for i := 0; i <= primeUpperBound; i++ {
		if big.NewInt(int64(i)).ProbablyPrime(0) {
			name = path.Join(dir, fmt.Sprintf(resolvedFileName, i))
			if current.count > 0 {
				current.to = name
				ranges = append(ranges, current)
			}
			current = &resolvedRange{from: name}
			batches[current.from] = make([]string, 0)
			content = fmt.Sprintf(resolvedContent, i)
		} else {
			name = path.Join(dir, fmt.Sprintf(ndjsonFileName, i))
			batches[current.from] = append(batches[current.from], name)
			current.count++
			content = fmt.Sprintf(ndjsonContent, i)
		}
		fs[name] = &fstest.MapFile{
			Data:    []byte(content),
			Mode:    0777,
			ModTime: time.Now(),
		}
	}
	return ranges, batches, nil
}

// readTimestamp retrieve the timestamp from a resolved timestamp file.
func readTimestamp(fs fstest.MapFS, path string) (hlc.Time, error) {
	parser := &cdcjson.NDJsonParser{}
	file, err := fs.Open(path)
	if err != nil {
		return hlc.Zero(), err
	}
	return parser.Resolved(file)
}
