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

package base

import "github.com/jackc/pgx/v5/pgxpool"

// DBInfo encapsulates metadata and a connection to a database.
type DBInfo struct {
	db      *pgxpool.Pool
	version string
}

// Pool returns the underlying database connection.
func (di DBInfo) Pool() *pgxpool.Pool { return di.db }

// Version returns the database version.
func (di DBInfo) Version() string { return di.version }
