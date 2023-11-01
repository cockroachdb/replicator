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

import (
	"net/http"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/diag"
	"github.com/cockroachdb/cdc-sink/internal/util/stdlogical"
)

// A Server receives incoming CockroachDB changefeed messages and
// applies them to a target cluster.
type Server struct {
	auth  types.Authenticator
	diags *diag.Diagnostics
	mux   *http.ServeMux
}

var (
	_ stdlogical.HasAuthenticator = (*Server)(nil)
	_ stdlogical.HasDiagnostics   = (*Server)(nil)
	_ stdlogical.HasServeMux      = (*Server)(nil)
)

// GetAuthenticator implements [stdlogical.HasAuthenticator].
func (s *Server) GetAuthenticator() types.Authenticator {
	return s.auth
}

// GetDiagnostics implements [stdlogical.HasDiagnostics].
func (s *Server) GetDiagnostics() *diag.Diagnostics {
	return s.diags
}

// GetServeMux implements [stdlogical.HasServeMux].
func (s *Server) GetServeMux() *http.ServeMux {
	return s.mux
}
