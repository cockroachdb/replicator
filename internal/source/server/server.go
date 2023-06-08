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

// Package server contains a generic HTTP server that installs
// the CDC listener.
package server

// This file contains code repackaged from main.go

import _ "net/http/pprof" // Register pprof debugging endpoints.

// A Server receives incoming CockroachDB changefeed messages and
// applies them to a target cluster.
type Server struct {
	stopped chan struct{}
}

// Stopped returns a channel that will be closed when the server has
// been shut down.
func (s *Server) Stopped() <-chan struct{} {
	return s.stopped
}
