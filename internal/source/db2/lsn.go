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

// lsn is the Log Sequence Number.
// It represents the offset, in bytes, of a log record from the beginning of a database log file
type lsn struct {
	Value [16]byte
}

type lsnRange struct {
	From *lsn
	To   *lsn
}

var _ stamp.Stamp = (*lsn)(nil)
var _ fmt.Stringer = (*lsn)(nil)

func newLSN(v []byte) (*lsn, error) {
	if len(v) == 0 {
		return lsnZero(), nil
	}
	if len(v) != 16 {
		return nil, errors.Errorf("invalid lsn %s %d", string(v), len(v))
	}
	return &lsn{
		Value: ([16]byte)(v),
	}, nil
}

// Less implements stamp.Stamp.
func (l *lsn) Less(other stamp.Stamp) bool {
	o := other.(*lsn)
	return bytes.Compare(l.Value[:], o.Value[:]) < 0
}

// Equal check if two log sequence numbers are the same.
func (l *lsn) Equal(other stamp.Stamp) bool {
	o := other.(*lsn)
	return bytes.Equal(l.Value[:], o.Value[:])
}

// String implements fmt.Stringer.
func (l *lsn) String() string {
	return fmt.Sprintf("%x", l.Value)
}

// lsnMemo is used to store the lsn as JSON object in the memo table.
type lsnMemo struct {
	Value []byte `json:"value,omitempty"`
}

func (l *lsn) MarshalJSON() ([]byte, error) {
	p := lsnMemo{Value: l.Value[:]}
	return json.Marshal(p)
}

func (l *lsn) UnmarshalJSON(data []byte) error {
	var p lsnMemo
	if err := json.Unmarshal(data, &p); err != nil {
		return errors.WithStack(err)
	}
	if len(p.Value) != 16 {
		return errors.Errorf("invalid lsn %s %d", string(data), len(p.Value))
	}
	l.Value = ([16]byte)(p.Value)
	return nil
}

// UnmarshalText supports CLI flags and default values.
func (l *lsn) UnmarshalText(data []byte) (err error) {
	if data == nil {
		l.Value = lsnZero().Value
		return
	}
	if len(data) != 32 {
		return errors.Errorf("invalid lsn %s", string(data))
	}
	var v []byte
	v, err = hex.DecodeString(string(data))
	l.Value = ([16]byte)(v)
	return
}

func lsnZero() *lsn {
	z := make([]byte, 16)
	return &lsn{
		Value: ([16]byte)(z),
	}
}
