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

package all

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAllSmoke(t *testing.T) {
	r := require.New(t)

	fixture, err := NewFixture(t, time.Minute)
	r.NoError(err)

	r.NotEmpty(fixture.StagingPool.Version)
	r.NotEmpty(fixture.TargetPool.Version)
}
