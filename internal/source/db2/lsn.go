// Copyright 2024 The Cockroach Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package db2

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	"github.com/pkg/errors"
)

// lsn represents the offset, in bytes, of a log record from the beginning of a database log file
type lsn struct {
	Value []byte
}

type lsnRange struct {
	from *lsn
	to   *lsn
}

var _ stamp.Stamp = (*lsn)(nil)
var _ fmt.Stringer = (*lsn)(nil)

const zero = "00000000000000000000000000000000"

// Less implements stamp.Stamp.
func (l *lsn) Less(other stamp.Stamp) bool {
	o := other.(*lsn)
	return bytes.Compare(l.Value, o.Value) < 0
}

// Equal check if two log sequence numbers are the same.
func (l *lsn) Equal(other stamp.Stamp) bool {
	o := other.(*lsn)
	return bytes.Equal(l.Value, o.Value)
}

// String implements fmt.Stringer.
func (l *lsn) String() string {
	return fmt.Sprintf("%x", l.Value)
}

type lsnMemo struct {
	Value []byte `json:"value,omitempty"`
}

func (l *lsn) MarshalJSON() ([]byte, error) {
	p := lsnMemo{Value: l.Value}
	return json.Marshal(p)
}

func (l *lsn) UnmarshalJSON(data []byte) error {
	var p lsnMemo
	if err := json.Unmarshal(data, &p); err != nil {
		return errors.WithStack(err)
	}
	l.Value = p.Value
	return nil
}

// UnmarshalText supports CLI flags and default values.
func (l *lsn) UnmarshalText(data []byte) (err error) {
	if len(data) == 0 {
		l.Value, _ = hex.DecodeString(zero)
	}
	l.Value, err = hex.DecodeString(string(data))
	return
}

func lsnZero() *lsn {
	z, _ := hex.DecodeString(zero)
	return &lsn{
		Value: z,
	}
}
