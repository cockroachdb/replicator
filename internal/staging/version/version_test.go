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

package version_test

import (
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/sinktest/all"
	"github.com/cockroachdb/cdc-sink/internal/staging/version"
	"github.com/stretchr/testify/require"
)

func TestChecker(t *testing.T) {
	r := require.New(t)

	fixture, err := all.NewFixture(t)
	r.NoError(err)

	checker := fixture.VersionChecker
	ctx := fixture.Context

	// Verify bootstrap.
	warnings, err := checker.Check(ctx)
	r.NoError(err)
	r.Empty(warnings)

	// Introduce a new version.
	version.Versions = append(version.Versions, version.Version{
		Info: "testing",
		PR:   1234,
	})

	warnings, err = checker.Check(ctx)
	r.NoError(err)
	r.Len(warnings, 1)

	// Perform schema change out of band.
	r.NoError(fixture.Memo.Put(ctx,
		fixture.StagingPool,
		"version-1234",
		[]byte(`{"state":"applied"}`)))

	warnings, err = checker.Check(ctx)
	r.NoError(err)
	r.Empty(warnings)
}
