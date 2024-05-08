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

package server

import (
	"fmt"
	"testing"
	"testing/fstest"

	"github.com/cockroachdb/replicator/internal/script"
	"github.com/cockroachdb/replicator/internal/sinktest/base"
	"github.com/cockroachdb/replicator/internal/source/cdc"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

// This test ensures that the database pools can be configured via
// the script API.
func TestScriptConfigureTarget(t *testing.T) {
	r := require.New(t)

	fixture, err := base.NewFixture(t)
	r.NoError(err)

	tsFile := fmt.Sprintf(`
import * as api from "cdc-sink@v1";
api.setOptions({
  "bindAddr": "127.0.0.1:0",
  "stagingSchema": %q,
  "targetConn": %q
});
`,
		// We'll use the staging schema since it's always CRDB.
		fixture.StagingDB.Idents(nil)[0].Raw(),
		fixture.StagingPool.ConnectionString,
	)

	cfg := &Config{
		CDC: cdc.Config{
			ScriptConfig: script.Config{
				MainPath: "/main.ts",
				FS:       fstest.MapFS{"main.ts": &fstest.MapFile{Data: []byte(tsFile)}},
			},
		},
	}
	cfg.Bind(pflag.NewFlagSet("testing", pflag.ContinueOnError))

	_, err = NewServer(fixture.Context, cfg)
	r.NoError(err)
}
