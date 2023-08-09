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

package cdc

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
)

const (
	// JSON operation values
	deleteFieldValue = `"delete"`
	insertFieldValue = `"insert"`
	updateFieldValue = `"update"`
)

// labels
var (
	crdbLabel  = ident.New("__crdb__")
	eventLabel = ident.New("__event__")
)

// Metadata contains a string representation of a timestamp,
// either as "Resolved" or "Updated".
type Metadata struct {
	Resolved string `json:"resolved"`
	Updated  string `json:"updated"`
}

// operationType is the type of the operation associated with the
// mutation: delete,insert or update.
type operationType int

const (
	unknownOp operationType = iota
	deleteOp
	insertOp
	updateOp
)

//go:generate go run golang.org/x/tools/cmd/stringer -type=operationType

// decodeOp reads the operation type from a serialized JSON value.
func decodeOp(op json.RawMessage) (operationType, error) {
	switch string(op) {
	case deleteFieldValue:
		return deleteOp, nil
	case insertFieldValue:
		return insertOp, nil
	case updateFieldValue:
		return updateOp, nil
	}
	return unknownOp, fmt.Errorf("unknown operation %s", op)
}

// decodeUpdatedTimestamp extracts the update hlc.Time from a serialized Metadata JSON object.
func decodeUpdatedTimestamp(data json.RawMessage) (hlc.Time, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	var time Metadata
	if err := dec.Decode(&time); err != nil {
		return hlc.Time{}, err
	}
	return hlc.Parse(time.Updated)
}

// queryPayload stores the payload sent by the client for
// a change feed that uses a query
type queryPayload struct {
	after     *ident.Map[json.RawMessage]
	keys      *ident.Map[int]
	keyValues []json.RawMessage
	operation operationType
	updated   hlc.Time
}

// AsMutation converts the QueryPayload into a types.Mutation
func (q *queryPayload) AsMutation() (types.Mutation, error) {
	var after json.RawMessage
	if q.operation != deleteOp {
		var err error
		after, err = json.Marshal(q.after)
		if err != nil {
			return types.Mutation{}, err
		}
	}
	key, err := json.Marshal(q.keyValues)
	if err != nil {
		return types.Mutation{}, err
	}
	return types.Mutation{
		Data: after,
		Key:  key,
		Time: q.updated,
	}, nil
}

// UnmarshalJSON reads a serialized JSON object,
// and extracts the updated timestamp, the operation, and values
// for all the remaining fields.
// If QueryPayload is initialized with Keys that are expected in the data,
// UnmarshalJSON will extract and store them in keyValues slice.
// Example:
// {"__event__": "insert", "pk" : 42, "v" : 9, "__crdb__": {"updated": "1.0"}}
func (q *queryPayload) UnmarshalJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	q.keyValues = make([]json.RawMessage, q.keys.Len())
	q.after = &ident.Map[json.RawMessage]{}
	if err := dec.Decode(&q.after); err != nil {
		return err
	}
	var v json.RawMessage
	var ok bool
	var err error
	// Process eventLabel.
	if v, ok = q.after.Get(eventLabel); !ok {
		return errors.Errorf(
			"CREATE CHANGEFEED must specify the %s colum set to op_event()",
			eventLabel.Raw())
	}
	if q.operation, err = decodeOp(v); err != nil {
		return fmt.Errorf("unable to decode operation type: %w", err)
	}
	q.after.Delete(eventLabel)

	// Process crdbLabel - it must contain the updated timestamp.
	if v, ok = q.after.Get(crdbLabel); !ok {
		return errors.Errorf("missing %s field", crdbLabel)
	}
	if q.updated, err = decodeUpdatedTimestamp(v); err != nil {
		return err
	}
	q.after.Delete(crdbLabel)

	// Process keys.
	return q.keys.Range(func(k ident.Ident, pos int) error {
		if v, ok = q.after.Get(k); !ok {
			return fmt.Errorf("expecting a value for key: %s", k)
		}
		q.keyValues[pos] = v
		return nil
	})
}
