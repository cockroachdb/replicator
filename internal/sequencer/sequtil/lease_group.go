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

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/stopper"
	log "github.com/sirupsen/logrus"
)

// LeaseGroup ensures that multiple sequencers do not operate on the
// same tables. This function will create a goroutine within the context
// that acquires a lease based on the group name. The callback will be
// executed as a goroutine within a suitably nested stopper.
func LeaseGroup(
	outer *stopper.Context,
	leases types.Leases,
	group *types.TableGroup,
	fn func(*stopper.Context, *types.TableGroup),
) {
	// Start a goroutine in the outer context.
	outer.Go(func() error {
		// Run this in a loop in case of non-renewal. This is likely
		// caused by database overload or any other case where we can't
		// run SQL in a timely fashion.
		for !outer.IsStopping() {
			// Acquire a lease.
			leases.Singleton(outer, fmt.Sprintf("sequtil.Lease.%s", group.Name),
				func(leaseContext context.Context) error {
					log.Tracef("acquired gloabal lease for %s", group.Name)
					defer log.Tracef("lost global lease for %s", group.Name)

					// Create a nested stopper whose lifetime is bound
					// to that of the lease.
					sub := stopper.WithContext(leaseContext)

					// Execute the callback from a goroutine. Tear down
					// the stopper once the main callback has exited.
					sub.Go(func() error {
						defer sub.Stop(time.Second)
						fn(sub, group)
						return nil
					})

					select {
					case <-sub.Stopping():
						// Defer release until all work has stopped.
						// This avoids spammy cancellation errors.
						<-sub.Done()
						return types.ErrCancelSingleton
					case <-sub.Done():
						// The lease has expired, we'll just exit.
						return sub.Err()
					}
				})
		}
		return nil
	})
}
