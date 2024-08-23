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

// Package sequtil contains sequencer utility methods.
package sequtil

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/types"
	log "github.com/sirupsen/logrus"
)

// LeaseGroup ensures that multiple sequencers do not operate on the
// same tables. This function will create a goroutine within the context
// that acquires a lease based on the tables in the group. The callback
// will be executed as a goroutine within a suitably nested stopper.
func LeaseGroup(
	outer *stopper.Context,
	leases types.Leases,
	group *types.TableGroup,
	fn func(*stopper.Context, *types.TableGroup),
) {
	// Start a goroutine in the outer context.
	outer.Go(func(outer *stopper.Context) error {
		// Acquire a lease on each table in the group.
		names := make([]string, len(group.Tables))
		for idx, table := range group.Tables {
			names[idx] = fmt.Sprintf("sequtil.Lease.%s", table.Canonical())
		}

		entry := log.WithFields(log.Fields{
			"enclosing": group.Enclosing,
			"name":      group.Name,
			"tables":    group.Tables,
		})

		// Run this in a loop in case of non-renewal. This is likely
		// caused by database overload or any other case where we can't
		// run SQL in a timely fashion.
		for !outer.IsStopping() {
			entry.Trace("waiting to acquire lease group")
			leases.Singleton(outer, names,
				func(leaseContext context.Context) error {
					entry.Trace("acquired lease group")
					defer entry.Trace("released lease group")

					for {
						// Create a nested stopper whose lifetime is bound
						// to that of the lease.
						sub := stopper.WithContext(leaseContext)

						// Execute the callback.
						fn(sub, group)

						// Tear down the stopper once the main callback
						// has exited.
						sub.Stop(time.Second)

						// Defer release until all work has stopped.
						// This avoids spammy cancellation errors.
						<-sub.Done()

						// If the outer context is being shut down,
						// release the lease.
						if outer.IsStopping() {
							entry.Trace("clean shutdown")
							return types.ErrCancelSingleton
						}

						// If the lease was canceled, return.
						if err := leaseContext.Err(); err != nil {
							return err
						}

						// We still hold the lease.
						entry.Trace("loop restarting")
					}
				})
		}
		return nil
	})
}
