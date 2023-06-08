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
	"github.com/jackc/pgx/v5/pgconn"
)

// rowsUnlocker will call an Unlock method once the Rows has been
// closed, exhausted, or enters an error state.
type rowsUnlocker struct {
	r pgx.Rows
	u interface{ Unlock() }
}

var _ pgx.Rows = (*rowsUnlocker)(nil)

func (r *rowsUnlocker) Conn() *pgx.Conn {
	return r.r.Conn()
}

func (r *rowsUnlocker) CommandTag() pgconn.CommandTag {
	return r.r.CommandTag()
}

func (r *rowsUnlocker) Close() {
	r.r.Close()
	r.unlock()
}

func (r *rowsUnlocker) Err() error {
	err := r.r.Err()
	if err != nil {
		r.unlock()
	}
	return err
}

func (r *rowsUnlocker) FieldDescriptions() []pgconn.FieldDescription {
	return r.r.FieldDescriptions()
}

func (r *rowsUnlocker) Next() bool {
	if r.r.Next() {
		return true
	}
	r.unlock()
	return false
}

func (r *rowsUnlocker) RawValues() [][]byte {
	return r.r.RawValues()
}

func (r *rowsUnlocker) Scan(dest ...any) error {
	err := r.r.Scan(dest...)
	if err != nil {
		r.unlock()
	}
	return err
}

func (r *rowsUnlocker) Values() ([]any, error) {
	ret, err := r.r.Values()
	if err != nil {
		r.unlock()
	}
	return ret, err
}

// unlock only acts once.
func (r *rowsUnlocker) unlock() {
	if u := r.u; u != nil {
		runtime.SetFinalizer(r, nil)
		u.Unlock()
		r.u = nil
	}
}
