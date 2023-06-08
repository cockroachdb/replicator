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

package fslogical

import (
	"strings"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
)

// A consistentPoint has two flavors to support backfilling
// and-or streaming modes.
type consistentPoint struct {
	// The last document ID read when backfilling.
	BackfillID string `json:"i,omitempty"`
	// A server-generated timestamp; only updated when a backfill has
	// completed or when we receive new data in streaming mode.
	Time time.Time `json:"t,omitempty"`
}

var _ stamp.Stamp = (*consistentPoint)(nil)

func streamPoint(ts time.Time) *consistentPoint {
	return &consistentPoint{
		Time: ts,
	}
}

// AsTime implements the optional logical.TimeStamp interface to aid in
// metrics reporting. The returned value is truncated to the preceding
// second, to accommodate varying levels of source timestamp resolution.
func (t *consistentPoint) AsTime() time.Time {
	return t.Time
}

// AsID is a convenience method to get the associated backfill document.
func (t *consistentPoint) AsID() string {
	return t.BackfillID
}

// IsZero returns true if the consistentPoint represents a zero value.
func (t *consistentPoint) IsZero() bool {
	return t.BackfillID == "" && t.Time.IsZero()
}

// Less implements stamp.Stamp.
func (t *consistentPoint) Less(other stamp.Stamp) bool {
	o := other.(*consistentPoint)

	tt := t.AsTime()
	ot := o.AsTime()
	if tt.Before(ot) {
		return true
	}
	if tt.After(ot) {
		return false
	}

	return strings.Compare(t.AsID(), o.AsID()) < 0
}
