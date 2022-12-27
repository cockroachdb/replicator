// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package hlc contains a trivial representation of CockroachDB's hybrid
// logical clock timestamp.
package hlc

// The code in this file is reworked from sink_table.go.
import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

// Time is a representation of the hybrid logical clock timestamp used
// by CockroachDB. This is an immutable value type, suitable for use as
// a map key.
type Time struct {
	nanos   int64
	logical int
}

// Compare two timestamps.
func Compare(a, b Time) int {
	if c := a.nanos - b.nanos; c != 0 {
		return int(c)
	}
	return a.logical - b.logical
}

// From constructs an HLC time from a wall time.
func From(t time.Time) Time {
	return Time{t.UnixNano(), 0}
}

// New constructs a new Time with wall and logical parts.
func New(nanos int64, logical int) Time {
	return Time{nanos, logical}
}

// Parse splits a timestmap of the format NNNN.LLL into an int64
// for the nanos and an int for the logical component.
func Parse(timestamp string) (Time, error) {
	splits := strings.Split(timestamp, ".")
	if len(splits) != 2 {
		return Time{}, errors.Errorf("can't parse timestamp %s", timestamp)
	}
	nanos, err := strconv.ParseInt(splits[0], 0, 0)
	if err != nil {
		return Time{}, err
	}
	if nanos < 0 {
		return Time{}, errors.Errorf("nanos must be greater than 0: %d", nanos)
	}
	logical, err := strconv.Atoi(splits[1])
	if len(splits[1]) != 10 && logical != 0 {
		return Time{}, errors.Errorf("logical part %q must be 10 digits or zero-valued", splits[1])
	}
	return Time{nanos, logical}, err
}

// Zero returns a zero-valued Time.
func Zero() Time {
	return Time{}
}

// Logical returns the logical counter.
func (t Time) Logical() int { return t.logical }

// Nanos returns the nanosecond wall time.
func (t Time) Nanos() int64 { return t.nanos }

// MarshalJSON represents the time as a JSON string. This is used when
// logging timestamps.
func (t Time) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.String())
}

// String returns the Time as a
func (t Time) String() string {
	return fmt.Sprintf("%d.%010d", t.nanos, t.logical)
}

// UnmarshalJSON restores the timestamp from a string representation.
func (t *Time) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	parsed, err := Parse(s)
	if err != nil {
		return err
	}
	*t = parsed
	return nil
}
