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
	"encoding/json"
	"fmt"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
)

// labels
var (
	beforeLabel = ident.New("cdc_prev")
	crdbLabel   = ident.New("__crdb__")
	eventLabel  = ident.New("__event__")
)

// Metadata contains a string representation of a timestamp,
// either as "Resolved" or "Updated".
type Metadata struct {
	Resolved string `json:"resolved"`
	Updated  string `json:"updated"`
}

// operationType is the type of the operation associated with the
// mutation: delete,insert or update.
//
//go:generate go run golang.org/x/tools/cmd/stringer -type=operationType
type operationType int

const (
	unknownOp operationType = iota
	deleteOp
	insertOp
	updateOp
	upsertOp
)

var encodedOpLookup = map[string]operationType{
	`"delete"`: deleteOp,
	`"insert"`: insertOp,
	`"update"`: updateOp,
	`"upsert"`: upsertOp,
}

// decodeUpdatedTimestamp extracts the update hlc.Time from a serialized Metadata JSON object.
func decodeUpdatedTimestamp(data json.RawMessage) (hlc.Time, error) {
	var time Metadata
	if err := json.Unmarshal(data, &time); err != nil {
		return hlc.Time{}, errors.Wrap(err, "could not parse MVCC timestamp")
	}
	return hlc.Parse(time.Updated)
}

// queryPayload stores the payload sent by the client for
// a change feed that uses a query
type queryPayload struct {
	after     *ident.Map[json.RawMessage]
	before    *ident.Map[json.RawMessage]
	keys      *ident.Map[int]
	keyValues []json.RawMessage
	operation operationType
	updated   hlc.Time
}

// AsMutation converts the QueryPayload into a types.Mutation
func (q *queryPayload) AsMutation() (types.Mutation, error) {
	// The JSON marshaling errors should fall into the never-happens
	// category since we unmarshalled the values below.
	var after, before json.RawMessage
	if q.operation != deleteOp {
		var err error
		after, err = json.Marshal(q.after)
		if err != nil {
			return types.Mutation{}, errors.WithStack(err)
		}
		if q.before != nil {
			before, err = json.Marshal(q.before)
			if err != nil {
				return types.Mutation{}, errors.WithStack(err)
			}
		}
	}
	key, err := json.Marshal(q.keyValues)
	if err != nil {
		return types.Mutation{}, errors.WithStack(err)
	}
	return types.Mutation{
		Before: before,
		Data:   after,
		Key:    key,
		Time:   q.updated,
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
	// Parse the payload into after. We'll perform some additional
	// extraction on the data momentarily.
	if err := json.Unmarshal(data, &q.after); err != nil {
		return errors.Wrap(err, "could not parse query payload")
	}

	// Process __event__ marker to determine if it's a deletion.
	if eventRaw, hasEvent := q.after.Get(eventLabel); !hasEvent {
		return errors.Errorf(
			"Add %[1]s column to changefeed: CREATE CHANGEFEED ... AS SELECT event_op() AS %[1]s",
			eventLabel.Raw())
	} else if op, validOp := encodedOpLookup[string(eventRaw)]; !validOp {
		return fmt.Errorf("unknown %s value: %s", eventLabel.Raw(), string(eventRaw))
	} else {
		q.after.Delete(eventLabel)
		q.operation = op
	}

	// Process __crdb__ to extract the MVCC timestamp.
	crdbRaw, hasCrdb := q.after.Get(crdbLabel)
	if !hasCrdb {
		return errors.Errorf("missing %s field", crdbLabel)
	}
	var err error
	if q.updated, err = decodeUpdatedTimestamp(crdbRaw); err != nil {
		return err
	}
	q.after.Delete(crdbLabel)

	// Process optional cdc_prev data for three-way merge.
	if beforeRaw, hasBefore := q.after.Get(beforeLabel); hasBefore {
		if err := json.Unmarshal(beforeRaw, &q.before); err != nil {
			return errors.Wrapf(err, "could not parse embeded %s payload", beforeLabel)
		}
		q.after.Delete(beforeLabel)
	}

	// Extract PK values.
	q.keyValues = make([]json.RawMessage, q.keys.Len())
	return q.keys.Range(func(k ident.Ident, pos int) error {
		v, ok := q.after.Get(k)
		if !ok {
			return fmt.Errorf("missing primary key: %s", k)
		}
		q.keyValues[pos] = v
		return nil
	})
}
