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

package serial

import (
	"runtime"

	"github.com/jackc/pgx/v5"
)

// rowUnlocker calls an Unlock method once it has been scanned.
// It can also be configured to always return a specific error.
type rowUnlocker struct {
	err error
	r   pgx.Row
	u   interface{ Unlock() }
}

var _ pgx.Row = (*rowUnlocker)(nil)

func (r *rowUnlocker) Scan(dest ...any) error {
	defer r.unlock()
	if err := r.err; err != nil {
		return err
	}
	return r.r.Scan(dest...)
}

// unlock only acts once.
func (r *rowUnlocker) unlock() {
	if r.u != nil {
		runtime.SetFinalizer(r, nil)
		r.u.Unlock()
		r.u = nil
	}
}
