// Copyright 2024 The Cockroach Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package oraclelogminer

import (
	"testing"
	"time"

	"github.com/cockroachdb/replicator/internal/sinkprod"
	"github.com/cockroachdb/replicator/internal/sinktest/base"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestStart(t *testing.T) {
	r := require.New(t)

	ctx := base.ProvideContext(t)

	const (
		sourceConnStr = `oracle://sys:password@127.0.0.1:1521/ORCLCDB?sysdba=true`
		targetConnStr = `postgres://root@localhost:26257/defaultdb?sslmode=disable`
	)

	fixture, err := base.NewFixture(t)
	r.NoError(err)

	crdbPool := fixture.TargetPool
	dbSchema := fixture.TargetSchema.Schema()

	poolCfg, err := pgx.ParseConfig(crdbPool.ConnectionString)
	r.NoError(err)
	newDB := dbSchema.Idents(nil)[0].Raw()
	poolCfg.Database = newDB
	connToRealSchema, err := pgx.ConnectConfig(ctx, poolCfg)

	_, err = connToRealSchema.Exec(ctx, `CREATE TABLE IF NOT EXISTS "USERTABLE" (
    "YCSB_KEY" VARCHAR(255) PRIMARY KEY,
    "FIELD0" VARCHAR(255),
    "FIELD1" VARCHAR(255),
    "FIELD2" VARCHAR(255),
    "FIELD3" VARCHAR(255),
    "FIELD4" VARCHAR(255),
    "FIELD5" VARCHAR(255),
    "FIELD6" VARCHAR(255),
    "FIELD7" VARCHAR(255),
    "FIELD8" VARCHAR(255),
    "FIELD9" VARCHAR(255),
    "SOURCE_TIMESTAMP" TIMESTAMP(6),
    "TARGET_TIMESTAMP" TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6)
);`)
	r.NoError(err)

	cfg := &Config{
		Staging: sinkprod.StagingConfig{
			Schema: fixture.StagingDB.Schema(),
		},
		Target: sinkprod.TargetConfig{
			CommonConfig: sinkprod.CommonConfig{
				Conn: crdbPool.ConnectionString,
			},
			ApplyTimeout: 10 * time.Minute, // Increase to make using the debugger easier.
		},
		SourceConn:   sourceConnStr,
		TargetSchema: dbSchema,
		SCN:          "1191665",
		TableUser:    "C##MYADMIN",
	}

	_, err = Start(ctx, cfg)
	r.NoError(err)
}
