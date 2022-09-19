// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package serial

import (
	"runtime"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgx/v4"
)

// rowsUnlocker will call an Unlock method once the Rows has been
// closed, exhausted, or enters an error state.
type rowsUnlocker struct {
	r pgx.Rows
	u interface{ Unlock() }
}

var _ pgx.Rows = (*rowsUnlocker)(nil)

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

func (r *rowsUnlocker) FieldDescriptions() []pgproto3.FieldDescription {
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
