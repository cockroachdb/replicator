// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sinktest

import "github.com/jackc/pgx/v4/pgxpool"

// DBInfo encapsulates metadata and a connection to a database.
type DBInfo struct {
	db      *pgxpool.Pool
	version string
}

// Pool returns the underlying database connection.
func (di DBInfo) Pool() *pgxpool.Pool { return di.db }

// Version returns the database version.
func (di DBInfo) Version() string { return di.version }
