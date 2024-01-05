// Copyright 2024 The Cockroach Authors
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

package db2

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractZip(t *testing.T) {
	tests := []struct {
		name  string
		files []string
		err   string
	}{
		{
			name:  "good.zip",
			files: []string{"", "/good", "/good/a", "/good/a/a.txt", "/good/a.txt"},
			err:   "",
		},
		{
			name:  "bad.zip",
			files: nil,
			err:   "zip contains sym links",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := assert.New(t)
			r := require.New(t)
			target, err := os.MkdirTemp(os.TempDir(), "db2*")
			r.NoError(err)
			log.Infof(target)
			defer os.RemoveAll(target)
			err = extractZip(path.Join("testdata", tt.name), target)
			if tt.err != "" {
				a.EqualError(err, tt.err)
				return
			}
			a.NoError(err)
			index := 0
			err = filepath.Walk(target,
				func(path string, info os.FileInfo, err error) error {
					if err != nil {
						return err
					}
					if index >= len(tt.files) {
						a.Fail("files don't match")
						return nil
					}
					fileName, _ := strings.CutPrefix(path, target)
					a.Equal(tt.files[index], fileName)
					index++
					return nil
				})
			r.NoError(err)
		})
	}
}

func TestExtractTar(t *testing.T) {
	tests := []struct {
		name  string
		files []string
		err   string
	}{
		{
			name:  "good.tar.gz",
			files: []string{"", "/good", "/good/a", "/good/a/a.txt", "/good/a.txt"},
			err:   "",
		},
		{
			name:  "bad.tar.gz",
			files: nil,
			err:   "tar contains parent directories",
		},
		{
			name:  "abs.tar.gz",
			files: nil,
			err:   "tar contains absolute paths",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := assert.New(t)
			r := require.New(t)
			target, err := os.MkdirTemp(os.TempDir(), "db2*")
			r.NoError(err)
			fmt.Println(target)
			defer os.RemoveAll(target)
			err = extractTar(path.Join("testdata", tt.name), target)
			if tt.err != "" {
				a.EqualError(err, tt.err)
				return
			}
			a.NoError(err)
			index := 0
			err = filepath.Walk(target,
				func(path string, info os.FileInfo, err error) error {
					if err != nil {
						return err
					}
					if index >= len(tt.files) {
						a.Fail("files don't match")
						return nil
					}
					fileName, _ := strings.CutPrefix(path, target)
					a.Equal(tt.files[index], fileName)
					index++
					return nil
				})
			r.NoError(err)
		})
	}
}
