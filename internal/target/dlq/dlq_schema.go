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

package dlq

import (
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
)

// cdc-sink doesn't create the DLQ table. Instead, we just validate that
// it contains a minimum set of expected columns. As a rule, cdc-sink
// does not create or modify schema elements in the target database.
// Futhermore, the DLQ-processing workflow can be arbitrarily complex
// and we can't predict how the user intends to operate on this data.
var expectedColumns = []ident.Ident{
	ident.New("dlq_name"),
	ident.New("source_nanos"),
	ident.New("source_logical"),
	ident.New("data_after"),
	ident.New("data_before"),
}

const (
	qBase     = `INSERT INTO %s (dlq_name, source_nanos, source_logical, data_after, data_before) VALUES `
	argsPG    = `($1, $2, $3, $4, $5)`
	argsMySQL = `(?, ?, ?, ?, ?)`
	argsOra   = `(:1, :2, :3, :4, :5)`
)

// These constants define a plausible reference schema that can be used
// to create a DLQ table. These strings are exported, since the tests
// are declared in the dlq_test package.
const (
	basicCRDBSchema = `CREATE TABLE %[1]s (
event UUID DEFAULT gen_random_uuid() PRIMARY KEY,
dlq_name TEXT NOT NULL,
source_nanos INT8 NOT NULL,
source_logical INT8 NOT NULL,
data_after JSONB NOT NULL,
data_before JSONB NOT NULL
)`
	basicMySQLSchema = `CREATE TABLE %[1]s (
event binary(16) DEFAULT (uuid()) PRIMARY KEY,
dlq_name TEXT NOT NULL,
source_nanos INT8 NOT NULL,
source_logical INT8 NOT NULL,
data_after JSON NOT NULL,
data_before JSON NOT NULL
)`
	basicOraSchema = `CREATE TABLE %[1]s (
event INTEGER GENERATED ALWAYS AS IDENTITY,
dlq_name VARCHAR(256) NOT NULL,
source_nanos INTEGER NOT NULL,
source_logical INTEGER NOT NULL,
data_after CLOB NOT NULL,
data_before CLOB NOT NULL
)`
	basicPGSchema = `CREATE TABLE %[1]s (
event SERIAL PRIMARY KEY,
dlq_name TEXT NOT NULL,
source_nanos INT8 NOT NULL,
source_logical INT8 NOT NULL,
data_after JSONB NOT NULL,
data_before JSONB NOT NULL
)`
)

// BasicSchemas is a collection of suggested schemas for the DLQ table.
// It is exported so that tests which require the DLQ can use the
// suggested schemas. See [all.Fixture.CreateDLQTable].
var BasicSchemas = map[types.Product]string{
	types.ProductCockroachDB: basicCRDBSchema,
	types.ProductMariaDB:     basicMySQLSchema,
	types.ProductMySQL:       basicMySQLSchema,
	types.ProductOracle:      basicOraSchema,
	types.ProductPostgreSQL:  basicPGSchema,
}
