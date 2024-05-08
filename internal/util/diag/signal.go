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

//go:build !windows

package diag

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/cockroachdb/replicator/internal/util/stopper"
	log "github.com/sirupsen/logrus"
)

// logOnSignal installs a signal handler that will log the current state
// of the diagnostics.
func logOnSignal(ctx *stopper.Context, d *Diagnostics) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGUSR1)
	ctx.Go(func() error {
		defer signal.Stop(ch)
		for {
			select {
			case <-ctx.Stopping():
				return nil
			case <-ch:
				log.WithFields(d.Payload(ctx)).Info()
			}
		}
	})
}
