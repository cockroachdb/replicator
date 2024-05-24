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
	"math/big"
	"math/rand"
	"path"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/source/objstore/bucket"
	"github.com/cockroachdb/replicator/internal/source/objstore/eventproc"
	"github.com/cockroachdb/replicator/internal/source/objstore/providers/local"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/cdcjson"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/pkg/errors"
	"github.com/psanford/memfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// Directory where to store the files in the in-memory filesystem
	baseDir = "mybucket"
	// How many files to write in the bucket
	// Use a prime number to guarantee the last file is a resolved timestamp.
	defaultUpperLimit = 2003

	// How often we should throw a transient error.
	defaultProbTransientError = 0.01
)

// templates
const (
	resolvedContent  = `{"resolved":"%d.0000000000"}`
	resolvedFileName = `%09d.RESOLVED`
	ndjsonFileName   = `%09d.ndjson`
	ndjsonContent    = `{"after": {"p": %[1]d}, "key": [%[1]d], "updated": "%[1]d.0000000000"}`
)

// mockBucket wraps a bucket.Reader and injects transient errors.
type mockBucket struct {
	impl               bucket.Reader
	probTransientError float32
}

var _ bucket.Reader = &mockBucket{}

// Open implements bucket.Reader.
func (m *mockBucket) Open(ctx *stopper.Context, path string) (io.ReadCloser, error) {
	if rand.Float32() <= m.probTransientError {
		return nil, bucket.ErrTransient
	}
	return m.impl.Open(ctx, path)
}

// Walk implements bucket.Reader.
func (m *mockBucket) Walk(
	ctx *stopper.Context,
	prefix string,
	options *bucket.WalkOptions,
	f func(*stopper.Context, string) error,
) error {
	if rand.Float32() <= m.probTransientError {
		return bucket.ErrTransient
	}
	return m.impl.Walk(ctx, prefix, options, f)
}

// mockConveyor is used in the tests to keep track of the latest timestamp.
// The remaining methods are not used and are not implemented.
type mockConveyor struct {
	bucketName string
	timestamp  hlc.Time
}

var _ Conveyor = &mockConveyor{}

// AcceptMultiBatch implements Conveyor.
func (m *mockConveyor) AcceptMultiBatch(
	context.Context, *types.MultiBatch, *types.AcceptOptions,
) error {
	panic("unimplemented")
}

// Advance implements Conveyor.
func (m *mockConveyor) Advance(_ context.Context, partition ident.Ident, ts hlc.Time) error {
	if partition.Raw() != m.bucketName {
		return errors.Errorf("invalid partition name %q; %q expected", partition, m.bucketName)
	}
	m.timestamp = ts
	return nil
}

// Ensure implements Conveyor.
func (m *mockConveyor) Ensure(context.Context, []ident.Ident) error {
	panic("unimplemented")
}

// mockMemo stores the state in memory.
type mockMemo struct {
	values sync.Map
}

var _ types.Memo = &mockMemo{}

// Get implements types.Memo.
func (m *mockMemo) Get(ctx context.Context, tx types.StagingQuerier, key string) ([]byte, error) {
	res, ok := m.values.Load(key)
	if !ok {
		return nil, nil
	}
	return res.([]byte), nil
}

// Put implements types.Memo.
func (m *mockMemo) Put(
	ctx context.Context, tx types.StagingQuerier, key string, value []byte,
) error {
	m.values.Store(key, value)
	return nil
}

// mockProcessor tracks the file names processed.
type mockProcessor struct {
	probTransientError float32
	mu                 struct {
		sync.Mutex
		batch []string
	}
}

var _ eventproc.Processor = &mockProcessor{}

// Process implements eventproc.Processor.
// It may return an ErrTransient to verify retry.
func (m *mockProcessor) Process(ctx *stopper.Context, path string) error {
	if rand.Float32() <= m.probTransientError {
		return bucket.ErrTransient
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.batch = append(m.mu.batch, path)
	return nil
}

func (m *mockProcessor) getSorted() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	slices.Sort(m.mu.batch)
	return m.mu.batch
}

// TestApply verifies we can process all the resolved timestamps and ndjson files.
func TestApply(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stop := stopper.WithContext(ctx)
	r := require.New(t)
	a := assert.New(t)
	rootFS := memfs.New()
	ranges, batches, err := generate(rootFS, baseDir, defaultUpperLimit)
	r.NoError(err)
	bucket, err := local.New(&local.Config{Filesystem: rootFS})
	r.NoError(err)
	expected := make([]string, 0, defaultUpperLimit)
	for _, v := range batches {
		expected = append(expected, v...)
	}
	slices.Sort(expected)
	conveyor := &mockConveyor{}
	processor := &mockProcessor{
		probTransientError: defaultProbTransientError,
	}
	processor.mu.batch = make([]string, 0, defaultUpperLimit)
	conn := &Conn{
		bucket: &mockBucket{
			impl:               bucket,
			probTransientError: defaultProbTransientError,
		},
		config: &Config{
			FetchDelay: time.Millisecond,
			Workers:    8,
		},
		conveyor:  conveyor,
		processor: processor,
		state: state{
			memo: &mockMemo{},
			key:  "test",
		},
	}
	stop.Go(
		func(ctx *stopper.Context) error {
			err := conn.apply(ctx, ".")
			r.NoError(err)
			return err
		})
	ticker := time.NewTicker(100 * time.Millisecond)
	// wait until we see all the files accumulated in the processor
	for {
		if len(processor.getSorted()) >= len(expected) {
			stop.Stop(time.Second)
			break
		}
		select {
		case <-stop.Stopping():
			r.Fail("process has stopped")
		case <-ticker.C:
		}

	}
	a.Equal(expected, processor.getSorted())
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
	rootFS := memfs.New()
	ranges, batches, err := generate(rootFS, baseDir, defaultUpperLimit)
	r.NoError(err)
	bucket, err := local.New(&local.Config{Filesystem: rootFS})
	r.NoError(err)
	conn := &Conn{
		bucket: &mockBucket{
			impl:               bucket,
			probTransientError: defaultProbTransientError,
		},
		config: &Config{
			FetchDelay: time.Millisecond,
			Workers:    8,
		},
		state: state{
			memo: &mockMemo{},
			key:  "test",
		},
	}
	for _, rg := range ranges {
		conveyor := &mockConveyor{}
		processor := &mockProcessor{
			probTransientError: defaultProbTransientError,
		}
		processor.mu.batch = make([]string, 0, rg.count)
		conn.processor = processor
		conn.conveyor = conveyor
		err := conn.applyRange(stop, ".", rg)
		r.NoError(err)
		a.Equal(batches[rg.from], processor.getSorted())
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
	rootFS := memfs.New()
	ranges, _, err := generate(rootFS, baseDir, defaultUpperLimit)
	r.NoError(err)
	bucket, err := local.New(&local.Config{Filesystem: rootFS})
	r.NoError(err)

	conn := &Conn{
		bucket: &mockBucket{
			impl:               bucket,
			probTransientError: defaultProbTransientError,
		},
		config: &Config{
			FetchDelay: time.Millisecond,
		},
		state: state{
			memo: &mockMemo{},
			key:  "test",
		},
	}
	from := ""
	for _, expected := range ranges {
		got, err := conn.findResolved(stop, ".", from)
		a.NoError(err)
		a.Equal(expected, got)
		from = got.to
	}
}

// generate test files and stores them into a in memory filesystem. The
// file names are generated with a padded, sequential natural number
// starting from 0. If the number is a prime, then we generate a
// resolved timestamp file, otherwise we generate a ndjson file that
// contains one single event. The primeUpperBound must be prime
// to ensure that the last file is resolved timestamp.
func generate(
	fs *memfs.FS, dir string, primeUpperBound int,
) ([]*resolvedRange, map[string][]string, error) {
	if !big.NewInt(int64(primeUpperBound)).ProbablyPrime(0) {
		return nil, nil, errors.Errorf("primeUpperBound must be prime. %d is not",
			primeUpperBound)
	}
	err := fs.MkdirAll(baseDir, 0777)
	if err != nil {
		return nil, nil, err
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
		err := fs.WriteFile(name, []byte(content), 0777)
		if err != nil {
			return nil, nil, err
		}
	}
	return ranges, batches, nil
}

// readTimestamp retrieve the timestamp from a resolved timestamp file.
func readTimestamp(fs *memfs.FS, path string) (hlc.Time, error) {
	parser := &cdcjson.NDJsonParser{}
	file, err := fs.Open(path)
	if err != nil {
		return hlc.Zero(), err
	}
	return parser.Resolved(file)
}
