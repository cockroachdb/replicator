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

	"github.com/jackc/pgx/v4"
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
