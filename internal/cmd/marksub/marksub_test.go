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

package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContentsEqual(t *testing.T) {
	r := require.New(t)

	eq, err := contentsEqual("testdata/data1.txt", "testdata/data1.txt")
	r.NoError(err)
	r.True(eq)
}

func TestProcessFiles(t *testing.T) {
	r := require.New(t)

	basepath, err := os.MkdirTemp("", "marksub")
	r.NoError(err)

	toCopy := []string{
		"test_input.md",
		"data1.txt",
		"data2.txt",
	}

	for _, path := range toCopy {
		in, err := os.Open(filepath.Join("testdata", path))
		r.NoError(err)

		out, err := os.Create(filepath.Join(basepath, path))
		r.NoError(err)

		_, err = io.Copy(out, in)
		r.NoError(err)
		r.NoError(in.Close())
		r.NoError(out.Close())
	}

	r.NoError(processFiles(context.Background(), basepath))

	expected, err := os.ReadFile(filepath.Join("testdata", "test_output.md"))
	r.NoError(err)

	// The rewritten temp file name won't change.
	actual, err := os.ReadFile(filepath.Join(basepath, "test_input.md"))
	r.NoError(err)

	r.Equal(string(expected), string(actual))
}

func TestSubstitute(t *testing.T) {
	tcs := []struct {
		name      string
		expectErr string
	}{
		{name: "no_op"},
		{name: "test"},
		{name: "bad_file_path", expectErr: "not in base path"},
		{name: "missing_ticks", expectErr: "unterminated input"},
		{name: "unterminated", expectErr: "unterminated input"},
	}

	base, err := os.Getwd()
	require.NoError(t, err)

	for idx, tc := range tcs {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			r := require.New(t)

			var buf bytes.Buffer
			err := substitute(base, fmt.Sprintf("%s/testdata/%s_input.md", base, tc.name), &buf)
			if tc.expectErr != "" {
				r.ErrorContains(err, tc.expectErr)
				return
			}

			expected, err := os.Open(fmt.Sprintf("%s/testdata/%s_output.md", base, tc.name))
			r.NoError(err)
			defer expected.Close()

			expectedBytes, err := io.ReadAll(expected)
			r.NoError(err)
			r.Equal(string(expectedBytes), buf.String())
		})
	}
}
