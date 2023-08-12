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

package schemawatch

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/google/uuid"
	ora "github.com/sijms/go-ora/v2"
	"github.com/stretchr/testify/require"
)

func TestOraParseHelpers(t *testing.T) {
	now := time.Now().UTC()

	tcs := []struct {
		typ      string
		input    string
		expected any
	}{
		{
			typ:   "RAW(16)",
			input: "C847E52A-2612-4B98-9835-B0F0A9FCCD2F",
			expected: func() []byte {
				u := uuid.MustParse("C847E52A-2612-4B98-9835-B0F0A9FCCD2F")
				// Can't slice without assignment to storage location.
				return u[:]
			}(),
		},
		{
			typ:      "TIMESTAMP(9) WITH TIME ZONE",
			input:    now.Format(time.RFC3339Nano),
			expected: ora.TimeStampTZ(now),
		},
		{
			typ:      "TIMESTAMP(9)",
			input:    now.Format("2006-01-02T15:04:05.999999999"),
			expected: ora.TimeStamp(now),
		},
		{
			typ:   "DATE",
			input: now.Format("2006-01-02"),
			expected: ora.TimeStamp(time.Date(
				now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)),
		},
	}

	for idx, tc := range tcs {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			r := require.New(t)

			helper := parseHelper(types.ProductOracle, tc.typ)
			r.NotNil(helper)
			ret, ok := helper(tc.input)
			r.True(ok)
			r.Equal(tc.expected, ret)
		})
	}
}
