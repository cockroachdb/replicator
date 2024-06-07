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

package eventproc

import (
	"path/filepath"
	"regexp"

	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/pkg/errors"
)

var (
	ndjsonRegex = regexp.MustCompile(`^(?P<prelude>([^-]+-){5})(?P<topic>.+)-(?P<schema_id>[^-]+).ndjson$`)
	ndjsonTopic = ndjsonRegex.SubexpIndex("topic")
)

// getTableName extracts the table name from a changefeed cloud storage ndjson file.
func getTableName(path string) (ident.Ident, error) {
	res := ndjsonRegex.FindStringSubmatch(filepath.Base(path))
	if res == nil {
		return ident.Ident{},
			errors.Wrapf(ErrInvalidPath, "unable to extract table name from %s", path)
	}
	return ident.New(res[ndjsonTopic]), nil
}
