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

package stage

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// This isn't in stage_test, since that file is in the stage_testing
// package and cannot access the private symbols.
func TestGzipHelpers(t *testing.T) {
	r := require.New(t)

	tcs := []struct {
		length int
	}{
		{0},
		{1},
		{gzipMinSize - 1},
		{gzipMinSize},
		{gzipMinSize + 1},
		{2 * gzipMinSize},
	}

	for _, tc := range tcs {
		t.Run(fmt.Sprintf("%d", tc.length), func(t *testing.T) {
			var buf bytes.Buffer
			for i := 0; buf.Len() < tc.length; i = (i + 1) % 10 {
				_, _ = fmt.Fprintf(&buf, "%d", i)
			}
			data := buf.Bytes()
			zipped, err := maybeGZip(data)
			r.NoError(err)
			if tc.length <= gzipMinSize {
				r.Equal(data, zipped)
			}
			t.Logf("%d -> %d", len(data), len(zipped))

			unzipped, err := maybeGunzip(zipped)
			r.NoError(err)
			r.Equal(data, unzipped)
		})
	}
}
