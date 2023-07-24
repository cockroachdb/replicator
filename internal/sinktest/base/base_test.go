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

package base

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBaseSmoke(t *testing.T) {
	r := require.New(t)

	fixture, cleanup, err := NewFixture()
	r.NoError(err)
	defer cleanup()

	r.NotNil(fixture.SourcePool.DB)
	r.NotEmpty(fixture.SourcePool.ConnectionString)
	r.NotEmpty(fixture.SourcePool.Version)

	r.NotNil(fixture.StagingPool.Pool)
	r.NotEmpty(fixture.StagingPool.ConnectionString)
	r.NotEmpty(fixture.StagingPool.Version)

	r.NotNil(fixture.TargetPool.DB)
	r.NotEmpty(fixture.TargetPool.ConnectionString)
	r.NotEmpty(fixture.TargetPool.Version)
}
