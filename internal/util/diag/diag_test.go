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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/staging/auth/broken"
	"github.com/cockroachdb/cdc-sink/internal/staging/auth/reject"
	"github.com/cockroachdb/cdc-sink/internal/staging/auth/trust"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/stretchr/testify/require"
)

func TestAuth(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d := New(stopper.WithContext(ctx))

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

func TestDiagnostics(t *testing.T) {
	r := require.New(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d := New(stopper.WithContext(ctx))

	var didCall atomic.Bool
	r.NoError(d.Register("foo", DiagnosticFn(func(context.Context) any {
		didCall.Store(true)
		return "XYZZY"
	})))

	// Re-reginstration should fail.
	r.ErrorContains(d.Register("foo", nil), "foo already registered")

	// Add a nested diagnostic.
	sub, err := d.Wrap("sub")
	r.NoError(err)
	r.NoError(sub.Register("bar", DiagnosticFn(func(ctx context.Context) any {
		return "baz"
	})))

	var buf bytes.Buffer
	r.NoError(d.Write(context.Background(), &buf, false))
	r.True(didCall.Load())

	// Spot-check the contents.
	var parsed map[string]any
	r.NoError(json.Unmarshal(buf.Bytes(), &parsed))
	r.Contains(parsed, "build") // Automatically added
	r.Contains(parsed, "cmd")
	r.Equal("XYZZY", parsed["foo"])
	r.Equal(map[string]any{"bar": "baz"}, parsed["sub"])

	before := len(d.mu.impls)
	d.Unregister("foo")
	r.Equal(before-1, len(d.mu.impls))

	// Not an error
	d.Unregister("unknown")
}
