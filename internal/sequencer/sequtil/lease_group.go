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
	"runtime"
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
	gracePeriod time.Duration,
	group *types.TableGroup,
	fn func(*stopper.Context, *types.TableGroup),
) {
	entry := log.WithFields(log.Fields{
		"enclosing": group.Enclosing,
		"name":      group.Name,
		"tables":    group.Tables,
	})

	// Start a goroutine in the outer context.
	outer.Go(func(outer *stopper.Context) error {
		// Acquire a lease on each table in the group.
		names := make([]string, len(group.Tables))
		for idx, table := range group.Tables {
			names[idx] = fmt.Sprintf("sequtil.Lease.%s", table.Canonical().Raw())
		}

		// If no table is specified via group.Tables, meaning we will
		// track the whole group rather than for individual tables, so
		// use the whole group name as the lease name.
		if len(names) == 0 {
			names = append(names, fmt.Sprintf("sequtil.Lease.%s", group.Name.Raw()))
		}

		// Run this in a loop in case of non-renewal. This is likely
		// caused by database overload or any other case where we can't
		// run SQL in a timely fashion.
		for !outer.IsStopping() {
			entry.Trace("waiting to acquire lease group")
			leases.Singleton(outer, names,
				func(leaseContext context.Context) error {
					entry.Debug("acquired lease group")
					defer entry.Debug("released lease group")

					for {
						// Create a nested stopper whose lifetime is bound
						// to that of the lease.
						sub := stopper.WithContext(leaseContext)

						// Allow the stopper chain to track this task.
						_ = sub.Call(func(ctx *stopper.Context) error {
							fn(ctx, group)
							return nil
						})

						// Shut down the nested stopper. The call to
						// Stop() is non-blocking.
						entry.Debugf("stopping; waiting for %d tasks to complete", sub.Len())
						sub.Stop(gracePeriod)

						select {
						case <-sub.Done(): // All task goroutines have exited.
						case <-time.After(gracePeriod):
							// If we have a stuck task, there's not much
							// we can do other than to kill the process.
							// Any caller waiting for the parent stopper
							// to became done would also be blocked on
							// waiting for the stuck task. We'll include
							// a complete stack dump to help with
							// diagnostics.
							buf := make([]byte, 1024*1024)
							buf = buf[:runtime.Stack(buf, true)]
							log.WithFields(log.Fields{
								"stack":     string(buf),
								"truncated": len(buf) == cap(buf),
							}).Fatalf("background task stuck after %s", gracePeriod)
						}

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
						entry.Debug("restarting")
					}
				})
			return nil
		}
		return nil
	})
}
