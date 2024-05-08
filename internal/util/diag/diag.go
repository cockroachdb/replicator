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

// Package diag contains a mechanism for cdc-sink components to report
// structured diagnostic information.
package diag

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"runtime/debug"
	"sync"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/httpauth"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/stopper"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Schema is passed to the authenticator.
var Schema = ident.MustSchema(ident.New("_"), ident.New("diag"))

// Diagnostic is implemented by any type that can provide structured
// diagnostic data for use in a support engagement.
type Diagnostic interface {
	// Diagnostic returns a json-serializable value that represent the
	// reportable state of the component.
	Diagnostic(context.Context) any
}

// A DiagnosticFn is passed to [Diagnostics.Register]. This type is use
// similarly to [http.HandlerFunc].
type DiagnosticFn func(context.Context) any

// Diagnostic implements Diagnostic.
func (c DiagnosticFn) Diagnostic(ctx context.Context) any {
	return c(ctx)
}

// Diagnostics serves as a registry for Diagnostic implementations.
type Diagnostics struct {
	mu struct {
		sync.RWMutex
		impls map[string]Diagnostic
	}
}

// New constructs a Diagnostics. The instance will write itself to the
// log stream if the process receives a SIGUSR1.
func New(ctx *stopper.Context) *Diagnostics {
	ret := &Diagnostics{}
	ret.mu.impls = map[string]Diagnostic{
		"build": DiagnosticFn(func(context.Context) any {
			bi, ok := debug.ReadBuildInfo()
			if !ok {
				return nil
			}
			info := make(map[string]string, len(bi.Settings))
			for _, s := range bi.Settings {
				info[s.Key] = s.Value
			}
			return info
		}),
		"cmd": DiagnosticFn(func(context.Context) any {
			return os.Args
		}),
	}

	logOnSignal(ctx, ret)

	return ret
}

// Handler returns an [http.Handler] to provide a summary report.
// The [Schema] value will be passed to the Authenticator.
func (d *Diagnostics) Handler(auth types.Authenticator) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		token := httpauth.Token(req)
		ok, err := auth.Check(req.Context(), Schema, token)
		if err != nil {
			log.WithError(err).Warn("could not authenticate request")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if !ok {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.Header().Set("content-type", "application/json")
		w.WriteHeader(http.StatusOK)

		if err := d.Write(req.Context(), w, true); err != nil {
			log.WithError(err).Warn("could not write diagnostics")
		}
	})
}

// Payload returns the diagnostic payload.
func (d *Diagnostics) Payload(ctx context.Context) map[string]any {
	ret := make(map[string]any)

	d.mu.RLock()
	defer d.mu.RUnlock()

	for key, impl := range d.mu.impls {
		ret[key] = impl.Diagnostic(ctx)
	}

	return ret
}

// Register adds a callback to generate a portion of the diagnostics
// output. The name will be used as the key in the emitted JSON literal,
// and the value will be the serialized form of the callback's return
// value. This method will return an error if the same name is
// registered twice.
//
// Note that the callback may be called at any time (possibly
// concurrently), so implementations should be written in a thread-safe
// manner.
func (d *Diagnostics) Register(name string, intf Diagnostic) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, conflict := d.mu.impls[name]; conflict {
		return errors.Errorf("%s already registered", name)
	}
	d.mu.impls[name] = intf
	return nil
}

// Unregister removes a registration.
func (d *Diagnostics) Unregister(name string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.mu.impls, name)
}

// Wrap returns a new instance of Diagnostics that will report its
// values as an embedded value.
func (d *Diagnostics) Wrap(name string) (*Diagnostics, error) {
	next := &Diagnostics{}
	next.mu.impls = map[string]Diagnostic{}

	return next, d.Register(name, DiagnosticFn(func(ctx context.Context) any {
		return next.Payload(ctx)
	}))
}

// Write the diagnostic report.
func (d *Diagnostics) Write(ctx context.Context, w io.Writer, pretty bool) error {
	p := d.Payload(ctx)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(true)
	if pretty {
		enc.SetIndent("", " ")
	}
	return enc.Encode(p)
}
