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
	"bytes"
	"encoding/json"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
)

// {
// "key": {
// 	 "schema": {
//  	  "type": "struct",
//  	  "fields": [
// 	    {
// 		  "type": "int32",
// 		  "optional": false,
// 		  "field": "i"
// 		}
// 	  ],
// 	  "optional": false,
// 	  "name": "cdc-connector.inventory.v.Key"
// 	},
// 	"payload": {
// 	  "i": 3
// 	 }
//  },
// 	"value": {
// 		"before": null,
// 		"after": {
// 		  "i": 3,
// 		  "v": "{\"k\":\"a\",\"v\":1}",
// 		  "r": 2
// 		},
// 		"source": {
// 		  "version": "2.5.0-SNAPSHOT",
// 		  "connector": "mysql",
// 		  "name": "cdc-connector",
// 		  "ts_ms": 1699713099000,
// 		  "snapshot": "false",
// 		  "db": "inventory",
// 		  "sequence": null,
// 		  "table": "js",
// 		  "server_id": 223344,
// 		  "gtid": null,
// 		  "file": "mysql-bin.000006",
// 		  "pos": 15920,
// 		  "row": 0,
// 		  "thread": 129,
// 		  "query": null
// 		},
// 		"op": "c",
// 		"ts_ms": 1699713101435,
// 		"transaction": null
// 	  }
// }

// kv: each debezium message has a key and a value.
type kv struct {
	Key   *key
	Value *value
}

// key has an optional schema and a payload.
// The payload contains a JSON object that can
// be converted to a types.Mutation.Key
type key struct {
	Schema  keySchema
	Payload map[string]json.RawMessage
}

// keySchema has a list of fields.
type keySchema struct {
	Fields []field
}

// field defines the type and the column name.
type field struct {
	Type  string
	Field string
}

// value contains before/after state for a row and additional metadata
// for the change event.
type value struct {
	After       json.RawMessage
	Before      json.RawMessage
	Source      *source
	Op          string
	Transaction string
	// Timestamp when the change event was received by the debezium connector.
	Timestamp    int64 `json:"ts_ms"`
	TableChanges []json.RawMessage
	DDL          string `json:"ddl"`
}

// source contains information about the source of the event.
// some fields are db specific (for instance mysql has gtids, sqlserver has commit_lsn)
// for now use the Timestamp of the transaction.
type source struct {
	Connector string
	// Timestamp when the event was committed in the source database
	Timestamp int64  `json:"ts_ms"`
	Table     string `json:"table"`
	Schema    string `json:"schema"`
	Database  string `json:"db"`
}

// a batch is simply a JSON array of debezium change events.
func parseBatch(in json.RawMessage) ([]*message, error) {
	var messages []json.RawMessage
	err := json.Unmarshal(in, &messages)
	if err != nil {
		return nil, err
	}
	events := make([]*message, len(messages))
	for i, ev := range messages {
		events[i], err = parseEvent(ev)
		if err != nil {
			return nil, err
		}
	}
	return events, nil
}

func getKey(key *key) (json.RawMessage, error) {
	res := make([]json.RawMessage, len(key.Schema.Fields))
	for i, v := range key.Schema.Fields {
		res[i] = key.Payload[v.Field]
	}
	return json.Marshal(res)
}

// parse a debezium request
func parseEvent(in json.RawMessage) (*message, error) {
	var msg kv
	dec := json.NewDecoder(bytes.NewReader(in))
	dec.UseNumber()
	if err := dec.Decode(&msg); err != nil {
		return nil, err
	}
	if msg.Key == nil {
		return nil, errors.New("missing key in message")
	}
	key, err := getKey(msg.Key)
	if err != nil {
		return nil, err
	}
	if msg.Value == nil {
		// tombstone don't have values.
		return &message{
			types.Mutation{
				Key: key,
			},
			sourceInfo{
				operationType: tombstoneOp,
			}}, nil
	}
	if msg.Value.Source == nil {
		return nil, errors.New("missing source info in message")
	}
	source := sourceInfo{
		connector: msg.Value.Source.Connector,
		table:     asTable(msg.Value.Source),
	}
	if msg.Value.DDL != "" || len(msg.Value.TableChanges) > 0 {
		// this is a change schema operation.
		source.operationType = changeSchemaOp
		return &message{
			source: source,
		}, nil
	}
	source.operationType = opMapping[msg.Value.Op]
	return &message{
		types.Mutation{
			Before: msg.Value.Before,
			Data:   msg.Value.After,
			Key:    key,
			Time:   hlc.New(int64(msg.Value.Source.Timestamp*int64(time.Millisecond)), 0),
		},
		source,
	}, nil
}

func asTable(source *source) ident.Table {
	if source.Schema != "" {
		return ident.NewTable(
			ident.MustSchema(ident.New(source.Database), ident.New(source.Schema)),
			ident.New(source.Table),
		)
	}
	return ident.NewTable(
		ident.MustSchema(ident.New(source.Database)),
		ident.New(source.Table),
	)
}
