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

package pglogical

import (
	"testing"

	"github.com/cockroachdb/replicator/internal/sinkprod"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/stretchr/testify/require"
)

func TestRenameParsing(t *testing.T) {
	r := require.New(t)
	cfg := &Config{
		Publication: "fake",
		Slot:        "fake",
		Target: sinkprod.TargetConfig{
			CommonConfig: sinkprod.CommonConfig{
				Conn: "fake",
			},
		},
		SourceConn:     "fake",
		TargetSchema:   ident.MustSchema(ident.New("fake")),
		renamePatterns: `{"table_.*":"table"}`,
	}
	r.NoError(cfg.Preflight())
	r.Len(cfg.Renames, 1)
}
