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

package eventproc

import (
	"bufio"
	"context"
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"
	"time"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/sinktest/recorder"
	"github.com/cockroachdb/replicator/internal/source/objstore/providers/local"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/cdcjson"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	ErrMock = errors.New("mock error")
)

// newAcceptor instantiate a recorder.Recorder.
// If an error is provided, it delegates to a types.MultiAcceptor
// that always return the given error for each call.
func newAcceptor(err error) *recorder.Recorder {
	var next types.MultiAcceptor
	if err != nil {
		next = &erroringAcceptor{err: err}
	}
	return &recorder.Recorder{
		Next: next,
	}
}

type erroringAcceptor struct {
	err error
}

// AcceptMultiBatch implements types.MultiAcceptor.
func (e *erroringAcceptor) AcceptMultiBatch(
	context.Context, *types.MultiBatch, *types.AcceptOptions,
) error {
	return e.err
}

// AcceptTableBatch implements types.MultiAcceptor.
func (e *erroringAcceptor) AcceptTableBatch(
	context.Context, *types.TableBatch, *types.AcceptOptions,
) error {
	return e.err
}

// AcceptTemporalBatch implements types.MultiAcceptor.
func (e *erroringAcceptor) AcceptTemporalBatch(
	context.Context, *types.TemporalBatch, *types.AcceptOptions,
) error {
	return e.err
}

var _ types.MultiAcceptor = &erroringAcceptor{}

type entry struct {
	content string
	path    string
}

func TestLocalProcess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stop := stopper.WithContext(ctx)
	bucketName := "bucket"
	content := []*entry{
		{
			// one mutation
			content: `
			{"after": {"p": 1, "v": "1"}, "before": null, "key": [1], "updated": "1.0"}`,
			path: `202405031553360274740000000000000-08779498965a12e2-1-2-00000000-mytable-2.ndjson`,
		},
		{ // many mutations
			content: `
			{"after": {"p": 1, "v": "1"}, "before": null, "key": [1], "updated": "1.0"}
			{"after": {"p": 5, "v": "1"}, "before": null, "key": [5], "updated": "1.0"}
			{"after": {"p": 2, "v": "1"}, "before": null, "key": [2], "updated": "1.0"}
			{"after": {"p": 20, "v": "1"}, "before": null, "key": [20], "updated": "2.0"}
			{"after": {"p": 4, "v": "1"}, "before": null, "key": [4], "updated": "3.0"}`,
			path: `202405031553360274750000000000000-08779498965a12e2-1-2-00000000-mytable-2.ndjson`,
		},
		{ // invalid path
			content: `1,2,3`,
			path:    `202405031553360274760000000000000-08779498965a12e2-1-2-00000000-mytable-2.cvs`,
		},
		{ // invalid mutation
			content: `
			{"after": {"p": 1, "v": "1"}, "before": null, "key": [1], "updated": "invalid"}`,
			path: `202405031553360274770000000000000-08779498965a12e2-1-2-00000000-mytable-2.ndjson`,
		},
	}
	r := require.New(t)
	fs := make(fstest.MapFS)
	err := addContent(fs, bucketName, content)
	r.NoError(err)
	bucket, err := local.New(fs)
	r.NoError(err)
	tests := []struct {
		name           string
		acceptor       *recorder.Recorder
		path           string
		wantTimestamps []hlc.Time
		wantErr        error
		wantTable      ident.Table
	}{
		{
			name:           "one",
			acceptor:       newAcceptor(nil),
			path:           `202405031553360274740000000000000-08779498965a12e2-1-2-00000000-mytable-2.ndjson`,
			wantTimestamps: []hlc.Time{hlc.New(1, 0)},
			wantTable:      ident.NewTable(ident.MustSchema(ident.Public), ident.New("mytable")),
		},
		{
			name:           "many",
			acceptor:       newAcceptor(nil),
			path:           `202405031553360274750000000000000-08779498965a12e2-1-2-00000000-mytable-2.ndjson`,
			wantTimestamps: []hlc.Time{hlc.New(1, 0), hlc.New(2, 0), hlc.New(3, 0)},
			wantTable:      ident.NewTable(ident.MustSchema(ident.Public), ident.New("mytable")),
		},
		{
			name:     "acceptor error",
			acceptor: newAcceptor(ErrMock),
			path:     `202405031553360274740000000000000-08779498965a12e2-1-2-00000000-mytable-2.ndjson`,
			wantErr:  ErrMock,
		},
		{
			name:     "invalid path",
			acceptor: newAcceptor(nil),
			path:     `202405031553360274760000000000000-08779498965a12e2-1-2-00000000-mytable-2.cvs`,
			wantErr:  ErrInvalidPath,
		},
		{
			name:     "invalid mutation",
			acceptor: newAcceptor(nil),
			path:     `202405031553360274770000000000000-08779498965a12e2-1-2-00000000-mytable-2.ndjson`,
			wantErr:  errors.New("can't parse timestamp invalid"),
		},
		{
			name:     "not found",
			acceptor: newAcceptor(nil),
			path:     `202405031553360274780000000000000-08779498965a12e2-1-2-00000000-mytable-2.ndjson`,
			wantErr:  errors.New("file does not exist"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := assert.New(t)
			parser, _ := cdcjson.New(bufio.MaxScanTokenSize)
			schema := ident.MustSchema(ident.Public)
			processor := NewLocal(tt.acceptor, bucket, parser, schema)
			err := processor.Process(stop, filepath.Join(bucketName, tt.path))
			if tt.wantErr != nil {
				a.ErrorContains(err, tt.wantErr.Error())
				return
			}
			a.NoError(err)
			// Make sure all the data in the batch has the correct table
			for _, call := range tt.acceptor.Calls() {
				for _, data := range call.Multi.Data {
					data.Data.Range(
						func(k ident.Table, v *types.TableBatch) error {
							a.Equal(tt.wantTable, k)
							return nil
						})
				}
			}
			// Verify timestamps.
			times := make([]hlc.Time, 0)
			for _, call := range tt.acceptor.Calls() {
				for time := range call.Multi.ByTime {
					times = append(times, time)
				}
			}
			slices.SortFunc(times, hlc.Compare)
			a.Equal(tt.wantTimestamps, times)
		})
	}
}

func addContent(fs fstest.MapFS, dir string, entries []*entry) error {
	for _, entry := range entries {
		fs[filepath.Join(dir, entry.path)] = &fstest.MapFile{
			Data:    []byte(entry.content),
			Mode:    0777,
			ModTime: time.Now(),
		}
	}
	return nil
}
