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

package sequtil

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/sinktest/all"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/stretchr/testify/require"
)

// TestRecreateLease ensures that LeaseGroup will restart if the
// acquired lease is lost.
func TestLeaseGroupExpiration(t *testing.T) {
	r := require.New(t)

	fixture, err := all.NewFixture(t)
	r.NoError(err)
	outer := fixture.Context

	var count atomic.Int32
	LeaseGroup(fixture.Context,
		fixture.Leases,
		time.Second, /* grace period */
		&types.TableGroup{
			Enclosing: fixture.TargetSchema.Schema(),
			Name:      ident.New("test"),
			Tables: []ident.Table{
				ident.NewTable(fixture.TargetSchema.Schema(), ident.New("test")),
			},
		},
		func(nested *stopper.Context, _ *types.TableGroup) {
			r.NotSame(outer, nested)

			switch count.Add(1) {
			case 1:
				// Retrieve the associated lease.
				lease, ok := nested.Value(types.LeaseKey{}).(types.Lease)
				r.True(ok)
				// This will cause the nested context to be canceled.
				lease.Release()
				<-nested.Stopping()
			case 2:
				// This will prevent another loop.
				outer.Stop(time.Second)
			default:
				r.Fail("should be called exactly twice")
			}
		})

	r.NoError(outer.Wait())
	r.Equal(int32(2), count.Load())
}
