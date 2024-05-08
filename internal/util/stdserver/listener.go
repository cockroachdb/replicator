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

package stdserver

import (
	"context"
	"net"

	"github.com/cockroachdb/replicator/internal/util/diag"
	"github.com/cockroachdb/replicator/internal/util/stopper"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Listener constructs the incoming network socket for the server.
func Listener(ctx *stopper.Context, config *Config, diags *diag.Diagnostics) (net.Listener, error) {
	// Start listening only when everything else is ready.
	l, err := net.Listen("tcp", config.BindAddr)
	if err != nil {
		return nil, errors.Wrapf(err, "could not bind to %q", config.BindAddr)
	}
	log.WithField("address", l.Addr()).Info("Server listening")
	if err := diags.Register("listener", diag.DiagnosticFn(func(context.Context) any {
		return l.Addr().String()
	})); err != nil {
		return nil, err
	}
	ctx.Defer(func() { _ = l.Close() })
	return l, nil
}
