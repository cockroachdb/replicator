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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func parse(t *testing.T, s string) *lsn {
	o, err := hex.DecodeString(s)
	require.NoError(t, err)
	return &lsn{Value: o}
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
		{"zero_one", zero, "00000000000000000000000000000001", true},
		{"one_zero", "00000000000000000000000000000001", zero, false},
		{"zero_max", zero, "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", true},
		{"max_zero", "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", zero, false},
		{"max", "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", false},
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
		{"zero_one", zero, "00000000000000000000000000000001", false},
		{"one_zero", "00000000000000000000000000000001", zero, false},
		{"zero_max", zero, "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", false},
		{"max_zero", "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", zero, false},
		{"max", "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", true},
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
		one  string
		want []byte
	}{
		{"zero", zero, []byte{0x7b, 0x22, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x3a, 0x22, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x3d, 0x3d, 0x22, 0x7d}},
		{"one", "00000000000000000000000000000001", []byte{0x7b, 0x22, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x3a, 0x22, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x41, 0x51, 0x3d, 0x3d, 0x22, 0x7d}},
		{"max", "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", []byte{0x7b, 0x22, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x3a, 0x22, 0x2f, 0x2f, 0x2f, 0x2f, 0x2f, 0x2f, 0x2f, 0x2f, 0x2f, 0x2f, 0x2f, 0x2f, 0x2f, 0x2f, 0x2f, 0x2f, 0x2f, 0x2f, 0x2f, 0x2f, 0x2f, 0x77, 0x3d, 0x3d, 0x22, 0x7d}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			one := parse(t, tt.one)
			got, err := one.MarshalJSON()
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
			// verify roundtrip
			rt := &lsn{}
			rt.UnmarshalJSON(got)
			assert.Equal(t, one.Value, rt.Value)
		})
	}
}

// TestUnmarshalText verifies unmarshalling from a text string.
func TestUnmarshalText(t *testing.T) {

	tests := []struct {
		name string
		text string
	}{
		{"zero", zero},
		{"one", "00000000000000000000000000000001"},
		{"max", "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			one := parse(t, tt.text)
			rt := &lsn{}
			rt.UnmarshalText([]byte(tt.text))
			assert.Equal(t, one.Value, rt.Value)
		})
	}
}
