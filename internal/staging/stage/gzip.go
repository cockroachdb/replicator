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
	"compress/gzip"
	"io"

	"github.com/pkg/errors"
)

// gzipMinSize disables compression for reasonable amounts of data. We
// don't expect this to be called very often, but it should provide us
// with some benefit for tables that have very wide rows or values.
const gzipMinSize = 1024

// maybeGZip compresses the given data if it is larger than gzipMinSize.
func maybeGZip(data []byte) ([]byte, error) {
	if len(data) <= gzipMinSize {
		return data, nil
	}

	var buf bytes.Buffer
	gzWriter := gzip.NewWriter(&buf)
	if _, err := gzWriter.Write(data); err != nil {
		return nil, errors.WithStack(err)
	}
	if err := gzWriter.Close(); err != nil {
		return nil, errors.WithStack(err)
	}
	if buf.Len() < len(data) {
		return buf.Bytes(), nil
	}
	return data, nil
}

// maybeGunzip looks for GZip magic numbers and decompresses the data if
// they're present. The magic numbers would not be present in JSON.
func maybeGunzip(data []byte) ([]byte, error) {
	if len(data) < 2 || data[0] != 0x1f || data[1] != 0x8b {
		return data, nil
	}
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return io.ReadAll(r)
}
