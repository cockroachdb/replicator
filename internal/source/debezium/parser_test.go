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

package debezium

import (
	"bufio"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	//go:embed testdata/begin.json
	begin string
	//go:embed testdata/ddl.json
	ddl string
	//go:embed testdata/delete.json
	delete string
	//go:embed testdata/end.json
	end string
	//go:embed testdata/insert.json
	insert string
	//go:embed testdata/snapshot.json
	snapshot string
	//go:embed testdata/tombstone.json
	tombstone string
	//go:embed testdata/txn.json
	txn string
	//go:embed testdata/update.json
	update string
	// json "null"
	jsonNull = json.RawMessage{0x6e, 0x75, 0x6c, 0x6c}
	jsTable  = ident.NewTable(ident.MustSchema(ident.New("inventory")), ident.New("js"))
	tTable   = ident.NewTable(ident.MustSchema(ident.New("inventory")), ident.New("t"))
)

func TestParse(t *testing.T) {

	tests := []struct {
		name    string
		in      string
		mut     types.Mutation
		info    sourceInfo
		txn     string
		wantErr string
	}{
		{
			name: "ddl",
			in:   ddl,
			info: sourceInfo{connector: "mysql", table: jsTable, operationType: changeSchemaOp},
		},
		{
			name: "begin",
			in:   begin,
			mut: types.Mutation{
				Time: hlc.New(1701815702567*1000*1000, 0),
			},
			info: sourceInfo{operationType: beginOp},
			txn:  "0000003c:00000660:0003",
		},
		{
			name: "end",
			in:   end,
			mut: types.Mutation{
				Time: hlc.New(1701815751163*1000*1000, 0),
			},
			info: sourceInfo{operationType: endOp},
			txn:  "0000003c:00000660:0003",
		},
		{
			name: "insert",
			in:   insert,
			mut: types.Mutation{
				Before: []byte(`null`),
				Data:   []byte(`{"i":3,"v":"{\"k\":\"a\",\"v\":1}","r":2.0}`),
				Key:    []byte(`[3]`),
				Time:   hlc.New(1699713099000*1000*1000, 0),
			},
			info: sourceInfo{connector: "mysql", table: jsTable, operationType: insertOp},
		},
		{
			name: "update",
			in:   update,
			mut: types.Mutation{
				Before: []byte(`{"i":3,"v":"{\"k\":\"a\",\"v\":1}","r":2.0}`),
				Data:   []byte(`{"i":3,"v":"{\"k\":\"a\",\"v\":1}","r":5.0}`),
				Key:    []byte(`[3]`),
				Time:   hlc.New(1699713102000*1000*1000, 0),
			},
			info: sourceInfo{connector: "mysql", table: jsTable, operationType: updateOp},
		},
		{
			name: "delete",
			in:   delete,
			mut: types.Mutation{
				Data:   []byte(`null`),
				Before: []byte(`{"i":3,"v":"{\"k\":\"a\",\"v\":1}","r":5.0}`),
				Key:    []byte(`[3]`),
				Time:   hlc.New(1699713109000*1000*1000, 0),
			},
			info: sourceInfo{connector: "mysql", table: jsTable, operationType: deleteOp},
		},
		{
			name: "snapshot",
			in:   snapshot,
			mut: types.Mutation{
				Before: []byte(`null`),
				Data:   []byte(`{"i":17,"j":1,"k":1}`),
				Key:    []byte(`[17,1]`),
				Time:   hlc.New(1699713050000*1000*1000, 0),
			},
			info: sourceInfo{connector: "mysql", table: tTable, operationType: snapshotOp},
		},
		{
			name: "tombstone",
			in:   tombstone,
			mut:  types.Mutation{Key: []byte(`[3]`)},
			info: sourceInfo{operationType: tombstoneOp},
		},
		{
			name: "txn",
			in:   txn,
			mut: types.Mutation{
				Before: []byte(`null`),
				Data:   []byte(`{"pk":2,"v":"a"}`),
				Key:    []byte(`[2]`),
				Time:   hlc.New(1701872914147*1000*1000, 0),
			},
			txn:  "0000003e:00007608:0003",
			info: sourceInfo{connector: "sqlserver", table: tTable, operationType: insertOp},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := assert.New(t)
			got, err := parseEvent([]byte(tt.in))
			if tt.wantErr != "" {
				a.Error(err)
				if err != nil {
					a.ErrorContains(err, tt.wantErr)
				}
			}
			a.NoError(err)
			a.Equal(tt.mut, got.mutation)
			a.Equal(tt.info, got.source)
			a.Equal(tt.txn, got.transactionID)
		})
	}
}

func muts(num int) []*message {
	res := make([]*message, num)
	for i := 0; i < num; i++ {
		key := fmt.Sprintf(`[%d,1]`, i+1)
		data := fmt.Sprintf(`{"i":%d,"j":1,"k":1}`, i+1)
		res[i] = &message{

			mutation: types.Mutation{
				Before: []byte(`null`),
				Data:   []byte(data),
				Key:    []byte(key),
				Time:   hlc.New(1699713050000*1000*1000, 0),
			},
			source: sourceInfo{connector: "mysql", table: tTable, operationType: snapshotOp},
		}
	}
	return res
}

// TestParseBatch verifies we can parse a batch of events, serialized as
// JSON array.
func TestParseBatch(t *testing.T) {
	tests := []struct {
		name    string
		want    []*message
		wantErr string
	}{
		{
			name: "insert_batch",
			want: muts(10),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := assert.New(t)
			r := require.New(t)
			buff, err := os.ReadFile(fmt.Sprintf("./testdata/%s.json", tt.name))
			r.NoError(err)
			got, err := parseBatch(buff)
			if tt.wantErr != "" {
				a.Error(err)
				if err != nil {
					a.ErrorContains(err, tt.wantErr)
				}
			}
			a.NoError(err)
			r.Equal(len(tt.want), len(got))
			for i, w := range tt.want {
				a.Equal(w.mutation, got[i].mutation)
				a.Equal(w.source, got[i].source)
			}
		})
	}
}

// TestParseStream verifies that we can process a stream coming from various connectors
func TestParseStream(t *testing.T) {
	for _, name := range []string{"mysql", "sqlserver", "oracle"} {
		t.Run(name, func(t *testing.T) {
			a := assert.New(t)
			r := require.New(t)
			file, err := os.Open(fmt.Sprintf("./testdata/%s.ndjson", name))
			r.NoError(err)
			defer file.Close()
			scanner := bufio.NewScanner(file)
			buf := make([]byte, 0, 64*1024)
			scanner.Buffer(buf, 1024*1024)
			linenum := 0
		scan:
			for scanner.Scan() {
				line := scanner.Bytes()
				linenum++

				events, err := parseBatch(line)
				r.NoError(err)
				for _, event := range events {
					switch event.source.operationType {
					case changeSchemaOp:
						a.Empty(event.mutation)
					case deleteOp:
						a.NotEmpty(event.mutation.Key)
						a.Equal(jsonNull, event.mutation.Data)
					case insertOp, snapshotOp:
						a.Equal(jsonNull, event.mutation.Before)
						a.NotEmpty(event.mutation.Key)
					case updateOp:
						a.NotEmpty(event.mutation.Key)
						a.NotEmpty(event.mutation.Before)
						a.NotEqual(jsonNull, event.mutation.Before)
						a.NotEmpty(event.mutation.Data)
						a.NotEqual(jsonNull, event.mutation.Data)
					case tombstoneOp:
						a.NotEmpty(event.mutation.Key)
						continue scan
					default:
						r.Failf("unknow op", "unknown operation on %d %s", linenum, string(line))
					}
					a.Equal(name, event.source.connector)
				}
			}
			r.NoError(scanner.Err())
			file.Close()
		})
	}
}
