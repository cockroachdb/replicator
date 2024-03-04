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

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
)

// labels
var (
	afterLabel  = ident.New("after")
	beforeLabel = ident.New("before")
	crdbLabel   = ident.New("__crdb__")
	eventLabel  = ident.New("__event__")
	prevLabel   = ident.New("cdc_prev")
	updated     = ident.New("updated")
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
	if q.after != nil && q.operation != deleteOp {
		var err error
		after, err = json.Marshal(q.after)
		if err != nil {
			return types.Mutation{}, errors.WithStack(err)
		}
	}
	if q.before != nil {
		var err error
		before, err = json.Marshal(q.before)
		if err != nil {
			return types.Mutation{}, errors.WithStack(err)
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
// Supports json format with envelope='raw' (default for queries) or envelope='wrapped'.
// Example raw:
// {"__event__": "insert", "pk" : 42, "v" : 9, "__crdb__": {"updated": "1.0"}}
// Example wrapped:
// {"after":{"__event__":"insert","k":2,"v":"a"},"before":null,"updated":"1.0"}
func (q *queryPayload) UnmarshalJSON(data []byte) error {
	// Parse the payload into msg. We'll perform some additional
	// extraction on the data momentarily.
	var msg *ident.Map[json.RawMessage]
	if err := json.Unmarshal(data, &msg); err != nil {
		return errors.Wrap(err, "could not parse query payload")
	}

	// Check if there is a __crdb__ property.
	// If it's there extract the MVCC timestamp.
	// And use the top level properties for after.
	crdbRaw, hasCrdb := msg.Get(crdbLabel)
	if hasCrdb {
		var err error
		if q.updated, err = decodeUpdatedTimestamp(crdbRaw); err != nil {
			return err
		}
		msg.Delete(crdbLabel)
		// Process optional cdc_prev data for three-way merge.
		if beforeRaw, hasBefore := msg.Get(prevLabel); hasBefore {
			if err := json.Unmarshal(beforeRaw, &q.before); err != nil {
				return errors.Wrapf(err, "could not parse embeded %s payload", prevLabel)
			}
			msg.Delete(prevLabel)
		}
		q.after = msg
	} else {
		// if not, try envelope=wrapped json format
		ts, ok := msg.Get(updated)
		if !ok {
			return errors.New("missing timestamp")
		}
		var tsAsString string
		if err := json.Unmarshal(ts, &tsAsString); err != nil {
			return errors.Wrap(err, "could not parse timestamp")
		}
		var err error
		if q.updated, err = hlc.Parse(tsAsString); err != nil {
			return err
		}
		if after, ok := msg.Get(afterLabel); ok {
			if err := json.Unmarshal(after, &q.after); err != nil {
				return errors.Wrap(err, "could not parse 'after' payload")
			}
		}
		if before, ok := msg.Get(beforeLabel); ok {
			if err := json.Unmarshal(before, &q.before); err != nil {
				return errors.Wrap(err, "could not parse 'before' payload")
			}
		}
	}
	// if the envelope=wrapped, q.after could be empty
	if q.after != nil {
		msg = q.after
		// Process __event__ marker to determine the operation.
		eventRaw, hasEvent := q.after.Get(eventLabel)
		if !hasEvent {
			return errors.Errorf(
				"Add %[1]s column to changefeed: CREATE CHANGEFEED ... AS SELECT event_op() AS %[1]s",
				eventLabel.Raw())
		}
		op, validOp := encodedOpLookup[string(eventRaw)]
		if !validOp {
			return errors.Errorf("unknown %s value: %s", eventLabel.Raw(), string(eventRaw))
		}
		q.after.Delete(eventLabel)
		q.operation = op
	} else {
		// we will get keys from q.before
		msg = q.before
		q.operation = deleteOp
	}
	// Extract PK values.
	q.keyValues = make([]json.RawMessage, q.keys.Len())
	return q.keys.Range(func(k ident.Ident, pos int) error {
		if msg == nil {
			return errors.New("missing primary keys")
		}
		v, ok := msg.Get(k)
		if !ok {
			return errors.Errorf("missing primary key: %s", k)
		}
		q.keyValues[pos] = v
		return nil
	})
}
