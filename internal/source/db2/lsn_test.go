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
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	zero  = "00000000000000000000000000000000"
	one   = "00000000000000000000000000000001"
	max   = "7FFFFFFFFFFFFFFF7FFFFFFFFFFFFFFF"
	zerob = [16]byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}
	oneb  = [16]byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1}
	maxb  = [16]byte{0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
)

func parse(t *testing.T, s string) *lsn {
	o, err := hex.DecodeString(s)
	require.NoError(t, err)
	lsn, err := newLSN(o)
	require.NoError(t, err)
	return lsn
}

// TestLess verifies stamp.Stamp.Less
func TestLess(t *testing.T) {
	tests := []struct {
		name  string
		one   string
		other string
		want  bool
	}{
		{"zero", zero, zero, false},
		{"zero_one", zero, one, true},
		{"one_zero", one, zero, false},
		{"zero_max", zero, max, true},
		{"max_zero", max, zero, false},
		{"max", max, max, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			one := parse(t, tt.one)
			other := parse(t, tt.other)
			got := one.Less(other)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestLess verifies equality between two Log Sequence Number
func TestEqual(t *testing.T) {
	tests := []struct {
		name  string
		one   string
		other string
		want  bool
	}{
		{"zero", zero, zero, true},
		{"zero_one", zero, one, false},
		{"one_zero", one, zero, false},
		{"zero_max", zero, max, false},
		{"max_zero", max, zero, false},
		{"max", max, max, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			one := parse(t, tt.one)
			other := parse(t, tt.other)
			got := one.Equal(other)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestJSON verifies marshalling to JSON and unmarshalling from JSON.
func TestJSON(t *testing.T) {
	tests := []struct {
		name string
		one  [16]byte
		want []byte
	}{
		{"zero", zerob, []byte{0x7b, 0x22, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x3a, 0x22, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x3d, 0x3d, 0x22, 0x7d}},
		{"one", oneb, []byte{0x7b, 0x22, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x3a, 0x22, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x51, 0x3d, 0x3d, 0x22, 0x7d}},
		{"max", maxb, []byte{0x7b, 0x22, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x3a, 0x22, 0x66, 0x2f, 0x2f, 0x2f, 0x2f, 0x2f, 0x2f, 0x2f, 0x2f, 0x2f, 0x39, 0x2f, 0x2f, 0x2f, 0x2f, 0x2f, 0x2f, 0x2f, 0x2f, 0x2f, 0x2f, 0x77, 0x3d, 0x3d, 0x22, 0x7d}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			one := &lsn{Value: tt.one}
			got, err := one.MarshalJSON()
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
			// verify roundtrip
			rt := &lsn{}
			err = rt.UnmarshalJSON(got)
			require.NoError(t, err)
			assert.Equal(t, one.Value, rt.Value)
		})
	}
}

// TestUnmarshalText verifies unmarshalling from a text string.
func TestUnmarshalText(t *testing.T) {
	tests := []struct {
		name string
		text string
		want [16]byte
		err  string
	}{
		{"zero", zero, zerob, ""},
		{"one", one, oneb, ""},
		{"max", max, maxb, ""},
		{"odd", "0", zerob, "invalid"},
		{"short", "00", zerob, "invalid"},
		{"long", strings.Repeat("0", 34), zerob, "invalid"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := assert.New(t)
			rt := &lsn{}
			err := rt.UnmarshalText([]byte(tt.text))
			if tt.err != "" {
				a.Error(err)
				a.ErrorContains(err, tt.err)
			} else {
				a.NoError(err)
				a.Equal(tt.want, rt.Value)
			}
		})
	}
}
