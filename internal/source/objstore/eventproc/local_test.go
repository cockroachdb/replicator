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
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/source/objstore/bucket"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/cdcjson"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/stretchr/testify/assert"
)

var (
	ErrMock = errors.New("mock error")
)

type mockAcceptor struct {
	batch *types.MultiBatch
	err   error
}

var _ Acceptor = &mockAcceptor{}

func newAcceptor(err error) *mockAcceptor {
	return &mockAcceptor{
		batch: &types.MultiBatch{},
		err:   err,
	}
}

// AcceptMultiBatch implements Acceptor.
func (m *mockAcceptor) AcceptMultiBatch(
	_ context.Context, batch *types.MultiBatch, _ *types.AcceptOptions,
) error {
	m.batch = batch
	return m.err
}

type mockBucket struct {
	content string
	err     error
	path    string
}

// Open implements bucket.Reader.
func (m *mockBucket) Open(ctx *stopper.Context, path string) (io.ReadCloser, error) {
	if m.path == path {
		return io.NopCloser(bytes.NewBuffer([]byte(m.content))), m.err
	}
	return nil, bucket.ErrNoSuchKey
}

// Walk implements bucket.Reader.
func (m *mockBucket) Walk(
	ctx *stopper.Context,
	prefix string,
	options *bucket.WalkOptions,
	f func(*stopper.Context, string) error,
) error {
	panic("unimplemented")
}

var _ bucket.Reader = &mockBucket{}

func TestLocalProcess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stop := stopper.WithContext(ctx)
	tests := []struct {
		name           string
		acceptor       *mockAcceptor
		bucket         bucket.Reader
		path           string
		wantTimestamps []hlc.Time
		wantErr        error
		wantTable      ident.Table
	}{
		{
			name:     "one",
			acceptor: newAcceptor(nil),
			bucket: &mockBucket{
				content: `
{"after": {"p": 1, "v": "1"}, "before": null, "key": [1], "updated": "1.0"}
`,
				path: `202405031553360274740000000000000-08779498965a12e2-1-2-00000000-mytable-2.ndjson`,
			},
			path:           `202405031553360274740000000000000-08779498965a12e2-1-2-00000000-mytable-2.ndjson`,
			wantTimestamps: []hlc.Time{hlc.New(1, 0)},
			wantTable:      ident.NewTable(ident.MustSchema(ident.Public), ident.New("mytable")),
		},
		{
			name:     "many",
			acceptor: newAcceptor(nil),
			bucket: &mockBucket{
				content: `
				{"after": {"p": 1, "v": "1"}, "before": null, "key": [1], "updated": "1.0"}
				{"after": {"p": 5, "v": "1"}, "before": null, "key": [5], "updated": "1.0"}
				{"after": {"p": 2, "v": "1"}, "before": null, "key": [2], "updated": "1.0"}
				{"after": {"p": 20, "v": "1"}, "before": null, "key": [20], "updated": "2.0"}
				{"after": {"p": 4, "v": "1"}, "before": null, "key": [4], "updated": "3.0"}`,
				path: `202405031553360274740000000000000-08779498965a12e2-1-2-00000000-mytable-2.ndjson`,
			},
			path:           `202405031553360274740000000000000-08779498965a12e2-1-2-00000000-mytable-2.ndjson`,
			wantTimestamps: []hlc.Time{hlc.New(1, 0), hlc.New(2, 0), hlc.New(3, 0)},
			wantTable:      ident.NewTable(ident.MustSchema(ident.Public), ident.New("mytable")),
		},
		{
			name:     "acceptor error",
			acceptor: newAcceptor(ErrMock),
			bucket: &mockBucket{
				content: `
{"after": {"p": 1, "v": "1"}, "before": null, "key": [1], "updated": "1.0"}
`,
				path: `202405031553360274740000000000000-08779498965a12e2-1-2-00000000-mytable-2.ndjson`,
			},
			path:    `202405031553360274740000000000000-08779498965a12e2-1-2-00000000-mytable-2.ndjson`,
			wantErr: ErrMock,
		},
		{
			name:     "transient bucket error",
			acceptor: newAcceptor(nil),
			bucket: &mockBucket{
				content: `
{"after": {"p": 1, "v": "1"}, "before": null, "key": [1], "updated": "1.0"}
`,
				path: `202405031553360274740000000000000-08779498965a12e2-1-2-00000000-mytable-2.ndjson`,
				err:  bucket.ErrTransient,
			},
			path:    `202405031553360274740000000000000-08779498965a12e2-1-2-00000000-mytable-2.ndjson`,
			wantErr: bucket.ErrTransient,
		},
		{
			name:     "invalid path",
			acceptor: newAcceptor(nil),
			bucket: &mockBucket{
				content: `
{"after": {"p": 1, "v": "1"}, "before": null, "key": [1], "updated": "1.0"}
`,
				path: `202405031553360274740000000000000-08779498965a12e2-1-2-00000000-mytable-2.cvs`,
			},
			path:    `202405031553360274740000000000000-08779498965a12e2-1-2-00000000-mytable-2.cvs`,
			wantErr: ErrInvalidPath,
		},
		{
			name:     "invalid mutation",
			acceptor: newAcceptor(nil),
			bucket: &mockBucket{
				content: `
{"after": {"p": 1, "v": "1"}, "before": null, "key": [1], "updated": "invalid"}
`,
				path: `202405031553360274740000000000000-08779498965a12e2-1-2-00000000-mytable-2.ndjson`,
			},
			path:    `202405031553360274740000000000000-08779498965a12e2-1-2-00000000-mytable-2.ndjson`,
			wantErr: errors.New("can't parse timestamp invalid"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := assert.New(t)
			parser, _ := cdcjson.New(bufio.MaxScanTokenSize)
			schema := ident.MustSchema(ident.Public)
			processor := Local(tt.acceptor, tt.bucket, parser, schema)
			err := processor.Process(stop, tt.path)
			if tt.wantErr != nil {
				a.ErrorContains(err, tt.wantErr.Error())
				return
			}
			a.NoError(err)
			// Make sure all the data in the batch has the correct table
			for _, data := range tt.acceptor.batch.Data {
				data.Data.Range(
					func(k ident.Table, v *types.TableBatch) error {
						a.Equal(tt.wantTable, k)
						return nil
					})
			}
			// Verify timestamps.
			times := make([]hlc.Time, 0)
			for time := range tt.acceptor.batch.ByTime {
				times = append(times, time)
			}
			slices.SortFunc(times, hlc.Compare)
			a.Equal(tt.wantTimestamps, times)
		})
	}
}
