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
	"math/big"
	"path"
	"slices"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/sinktest/mocks"
	"github.com/cockroachdb/replicator/internal/source/objstore/eventproc"
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
	ndjsonFileName   = `%s-0000-0-00-00000000-table-1.ndjson`
	resolvedContent  = `{"resolved":"%d.0000000000"}`
	resolvedFileName = `%s.RESOLVED`
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
	conn, collector, err := buildConn(rootFS, defaultProbTransientError)
	r.NoError(err)
	stop.Go(
		func(ctx *stopper.Context) error {
			return conn.apply(ctx, baseDir)
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

// TestApplyWithinRange verifies we can process all the resolved timestamps
// and ndjson files within a specified timestamp range.
func TestApplyWithinRange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stop := stopper.WithContext(ctx)
	r := require.New(t)
	a := assert.New(t)
	rootFS := make(fstest.MapFS)
	low := int(defaultUpperLimit / 4)
	high := int(defaultUpperLimit / 2)
	ranges, _, err := generate(rootFS, baseDir, defaultUpperLimit)
	r.NoError(err)
	timeRange := timeRange(low, high)
	conn, tracker, err := buildTimestampVerifier(rootFS)
	r.NoError(err)
	// Configure the time range we are interested in.
	conn.config.MinTimestamp = timeRange.Min()
	conn.config.timeRange = timeRange
	stop.Go(
		func(ctx *stopper.Context) error {
			err := conn.apply(ctx, baseDir)
			return err
		})
	ticker := time.NewTicker(100 * time.Millisecond)
	// wait until we see all the last timestamp
	for {
		lastTimestamp, err := conn.state.getLast(ctx, nil)
		r.NoError(err)
		if lastTimestamp == ranges[len(ranges)-1].to {
			stop.Stop(time.Second)
			break
		}
		select {
		case <-stop.Stopping():
			r.Fail("process has stopped")
		case <-ticker.C:
		}
	}
	// Verify we got all the timestamps.
	for i := low; i <= high; i++ {
		if !isPrime(i) {
			_, ok := tracker.timeRange.Load(hlc.New(timestamp(i).UnixNano(), 0))
			a.True(ok)
		}
	}
	// Verify all timestamps are within the range.
	tracker.timeRange.Range(func(t any, _ any) bool {
		timestamp, ok := t.(hlc.Time)
		r.True(ok)
		a.True(timeRange.Contains(timestamp))
		return true
	})

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
	conn, _, err := buildConn(rootFS, defaultProbTransientError)
	r.NoError(err)
	for _, rg := range ranges {
		conveyor := &timestampTracker{}
		collector := mocks.NewCollector(defaultUpperLimit)
		conn.processor = mocks.NewChaosProcessor(collector, defaultProbTransientError)
		conn.conveyor = conveyor
		err := conn.applyRange(stop, baseDir, rg)
		r.NoError(err)
		a.Equal(batches[rg.from], collector.GetSorted())
		timestamp, err := readTimestamp(rootFS, rg.to)
		r.NoError(err)
		a.Equal(conveyor.getCheckPoint(), timestamp)
	}
}

func TestFilePrefix(t *testing.T) {
	r := require.New(t)
	rootFS := make(fstest.MapFS)
	conn, _, err := buildConn(rootFS, defaultProbTransientError)
	conn.config.bucketName = "bucket"
	r.NoError(err)
	tests := []struct {
		name      string
		format    PartitionFormat
		timestamp string
		want      string
		wantErr   string
	}{
		{
			name:      "flat",
			format:    Flat,
			timestamp: "20240101101020",
			want:      "bucket/dir/20240101101020",
		},
		{
			name:      "daily",
			format:    Daily,
			timestamp: "20240105101020",
			want:      "bucket/dir/2024-01-05/20240105101020",
		},
		{
			name:      "hourly am",
			format:    Hourly,
			timestamp: "20240105101020",
			want:      "bucket/dir/2024-01-05/10/20240105101020",
		},
		{
			name:      "hourly pm",
			format:    Hourly,
			timestamp: "20240105151020",
			want:      "bucket/dir/2024-01-05/15/20240105151020",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			a := require.New(t)
			conn.config.PartitionFormat = test.format
			timestamp, err := time.Parse("20060102150405", test.timestamp)
			a.NoError(err)
			got, err := conn.filePrefix("dir", timestamp)
			if test.wantErr != "" {
				a.ErrorContains(err, test.wantErr)
				return
			}
			a.NoError(err)
			a.Equal(test.want, got)
		})
	}
}

// TestMinTimestamp verifies that with a min timestamp, we return
// a range that includes a file that starts with the min timestamp prefix
// and ends with the first resolved timestamp found after that.
func TestMinTimestamp(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stop := stopper.WithContext(ctx)
	r := require.New(t)
	rootFS := make(fstest.MapFS)
	_, _, err := generate(rootFS, baseDir, defaultUpperLimit)
	r.NoError(err)
	conn, _, err := buildConn(rootFS, 0)
	r.NoError(err)
	r.GreaterOrEqual(defaultUpperLimit, 1012)
	tests := []struct {
		name          string
		timeInSeconds int
		want          string
	}{
		{
			name:          "ten",
			timeInSeconds: 10,
			want:          filename(baseDir, 11, resolvedFileName),
		},
		{
			name:          "thirteen",
			timeInSeconds: 13,
			want:          filename(baseDir, 17, resolvedFileName),
		},
		{
			name:          "fifteen",
			timeInSeconds: 15,
			want:          filename(baseDir, 17, resolvedFileName),
		},
		{
			name:          "fiftyfive",
			timeInSeconds: 55,
			want:          filename(baseDir, 59, resolvedFileName),
		},
		{
			name:          "onesixty",
			timeInSeconds: 160,
			want:          filename(baseDir, 163, resolvedFileName),
		},
		{
			name:          "onethousandtwelve",
			timeInSeconds: 1012,
			want:          filename(baseDir, 1013, resolvedFileName),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			a := require.New(t)
			timestamp := hlc.New(timestamp(test.timeInSeconds).UnixNano(), 0)
			conn.config.PartitionFormat = Daily
			conn.config.MinTimestamp = timestamp
			min, err := conn.filePrefix(baseDir, time.Unix(0, timestamp.Nanos()).UTC())
			a.NoError(err)
			got, err := conn.findResolved(stop, baseDir, min)
			a.NoError(err)
			a.Equal(test.want, got.to)
			// if the first range is empty (we found a resolved timestamp file immediately),
			// we skip it, and return the next range, which includes a resolved timestamp file
			// as the lower bound.
			if isPrime(test.timeInSeconds) {
				a.Equal(filename(baseDir, test.timeInSeconds, resolvedFileName), got.from)
			} else {
				a.Equal(min, got.from)
			}
		})
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
	conn, _, err := buildConn(rootFS, defaultProbTransientError)
	r.NoError(err)

	from := ""
	for _, expected := range ranges {
		got, err := conn.findResolved(stop, baseDir, from)
		a.NoError(err)
		a.Equal(expected, got)
		from = got.to
	}
}

// timestampTracker is used in the tests to keep track of the latest timestamp.
// It is injected as a Conveyor.
type timestampTracker struct {
	bucketName string
	timeRange  sync.Map
	mu         struct {
		sync.RWMutex
		checkpoint hlc.Time
	}
}

var _ Conveyor = &timestampTracker{}

// AcceptMultiBatch implements Conveyor.
func (m *timestampTracker) AcceptMultiBatch(
	_ context.Context, batch *types.MultiBatch, _ *types.AcceptOptions,
) error {
	for timestamp := range batch.ByTime {
		m.timeRange.Store(timestamp, true)
	}
	return nil
}

// Advance implements Conveyor.
func (m *timestampTracker) Advance(_ context.Context, partition ident.Ident, ts hlc.Time) error {
	if partition.Raw() != m.bucketName {
		return errors.Errorf("invalid partition name %q; %q expected", partition, m.bucketName)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.checkpoint = ts
	return nil
}

// Ensure implements Conveyor.
func (m *timestampTracker) Ensure(context.Context, []ident.Ident) error {
	panic("unimplemented")
}

func (m *timestampTracker) getCheckPoint() hlc.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.mu.checkpoint
}

// buildConn returns a connection for testing purposes.
// The connection is backed by a in-memory bucket, and a processor
// that collects the file names read from the bucket and processed.
func buildConn(fs fs.FS, probTransientError float32) (*Conn, *mocks.Collector, error) {
	bucket, err := local.New(fs)
	if err != nil {
		return nil, nil, err
	}
	conveyor := &timestampTracker{}
	collector := mocks.NewCollector(defaultUpperLimit)
	return &Conn{
		bucket: mocks.NewChaosBucket(bucket, probTransientError),
		config: &Config{
			FetchDelay: time.Millisecond,
			Workers:    8,
			timeRange:  hlc.RangeIncluding(hlc.Zero(), hlc.New(math.MaxInt64, 0)),
		},
		conveyor:  conveyor,
		processor: mocks.NewChaosProcessor(collector, probTransientError),
		state: state{
			memo: &memo.Memory{},
			key:  "test",
		},
	}, collector, nil
}

// buildTimestampVerifier returns a connection for testing purposes.
// The connection provides a processor that tracks the mutation timestamps.
func buildTimestampVerifier(fs fs.FS) (*Conn, *timestampTracker, error) {
	bucket, err := local.New(fs)
	if err != nil {
		return nil, nil, err
	}
	conveyor := &timestampTracker{}
	parser, err := cdcjson.New(bufio.MaxScanTokenSize)
	if err != nil {
		return nil, nil, err
	}
	processor := eventproc.NewLocal(conveyor, bucket, parser, ident.MustSchema(ident.Public))
	return &Conn{
		bucket: bucket,
		config: &Config{
			FetchDelay: time.Millisecond,
			Workers:    8,
		},
		conveyor:  conveyor,
		processor: processor,
		state: state{
			memo: &memo.Memory{},
			key:  "test",
		},
	}, conveyor, nil
}

// filename returns the Nth file name.
// the Nth file has N seconds from
// 2024-01-01T01:01:00+00 + N nanoseconds.
func filename(dir string, n int, pattern string) string {
	timestamp := timestamp(n)
	prefix := path.Join(dir,
		timestamp.Format("2006-01-02"),
		fmt.Sprintf("%s%09d.0", timestamp.Format("20060102150405"), n),
	)
	return fmt.Sprintf(pattern, prefix)
}

// generate test files and stores them into a in memory filesystem.
// If the number is a prime, then we generate a
// resolved timestamp file, otherwise we generate a ndjson file that
// contains one single event. The primeUpperBound must be prime
// to ensure that the last file is resolved timestamp.
// The format of the file is defined by the filename function.
func generate(
	fs fstest.MapFS, dir string, primeUpperBound int,
) ([]*resolvedRange, map[string][]string, error) {
	if !isPrime(primeUpperBound) {
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
		timestamp := timestamp(i)
		if isPrime(i) {
			name = filename(dir, i, resolvedFileName)
			if current.count > 0 {
				current.to = name
				ranges = append(ranges, current)
			}
			current = &resolvedRange{from: name}
			batches[current.from] = make([]string, 0)
			content = fmt.Sprintf(resolvedContent, timestamp.UnixNano())
		} else {
			name = filename(dir, i, ndjsonFileName)
			batches[current.from] = append(batches[current.from], name)
			current.count++
			content = fmt.Sprintf(ndjsonContent, timestamp.UnixNano())
		}
		fs[name] = &fstest.MapFile{
			Data:    []byte(content),
			Mode:    0777,
			ModTime: time.Now(),
		}
	}
	return ranges, batches, nil
}

func isPrime(i int) bool {
	return big.NewInt(int64(i)).ProbablyPrime(0)
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

func timeRange(from int, to int) hlc.Range {
	return hlc.RangeIncluding(
		hlc.New(timestamp(from).UnixNano(), 0),
		hlc.New(timestamp(to).UnixNano(), 0),
	)
}

func timestamp(seconds int) time.Time {
	return time.Date(2024, 1, 1, 1, 1, seconds, 0, time.UTC)
}
