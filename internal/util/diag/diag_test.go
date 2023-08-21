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

package diag

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/staging/auth/broken"
	"github.com/cockroachdb/cdc-sink/internal/staging/auth/reject"
	"github.com/cockroachdb/cdc-sink/internal/staging/auth/trust"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/stretchr/testify/require"
)

func TestDiagnostics(t *testing.T) {
	r := require.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d, cancel := New(ctx)
	defer cancel()

	var didCall atomic.Bool
	r.NoError(d.Register("foo", DiagnosticFn(func(context.Context) any {
		didCall.Store(true)
		return "XYZZY"
	})))
	r.ErrorContains(d.Register("foo", nil), "foo already registered")

	var buf strings.Builder
	r.NoError(d.Write(context.Background(), &buf, false))
	r.True(didCall.Load())
	// The exact contents are sensitive to the build.
	r.Contains(buf.String(), "XYZZY")
}

func TestAuth(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d, cancel := New(ctx)
	defer cancel()

	tcs := []struct {
		auth   types.Authenticator
		expect int
	}{
		{
			auth:   trust.New(),
			expect: http.StatusOK,
		},
		{
			auth:   reject.New(),
			expect: http.StatusForbidden,
		},
		{
			auth:   broken.New(),
			expect: http.StatusInternalServerError,
		},
	}

	for idx, tc := range tcs {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			r := require.New(t)
			// The path is irrelevant here.
			req := httptest.NewRequest(http.MethodGet, "/_/diag", nil)
			w := httptest.NewRecorder()

			d.Handler(tc.auth).ServeHTTP(w, req)
			r.Equal(tc.expect, w.Code)
		})
	}
}
