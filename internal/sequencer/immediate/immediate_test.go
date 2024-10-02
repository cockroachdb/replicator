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

package immediate_test

import (
	"testing"

	"github.com/cockroachdb/replicator/internal/sequencer"
	"github.com/cockroachdb/replicator/internal/sequencer/seqtest"
	"github.com/cockroachdb/replicator/internal/sinktest/all"
	"github.com/stretchr/testify/require"
)

func TestImmediate(t *testing.T) {
	seqtest.CheckSequencer(t,
		&all.WorkloadConfig{
			DisableFK: true,
		},
		func(t *testing.T, fixture *all.Fixture, seqFixture *seqtest.Fixture) sequencer.Sequencer {
			seq, err := seqFixture.Immediate.Wrap(fixture.Context, seqFixture.Core)
			require.NoError(t, err)
			return seq
		},
		func(t *testing.T, check *seqtest.Check) {})
}
