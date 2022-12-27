// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
