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
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/replicator/internal/sinkprod"
	"github.com/cockroachdb/replicator/internal/sinktest/all"
	"github.com/cockroachdb/replicator/internal/sinktest/base"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/retry"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type execStmtWithExpectRes struct {
	description string
	execStmts   []string
	expectRes   [][]interface{}
}

type ingestionTestCase struct {
	description  string
	createTblSrc string
	createTblTgt string
	queryTblTgt  string

	execWithRes []execStmtWithExpectRes
}

func TestMain(m *testing.M) {
	all.IntegrationMain(m, all.OracleName)
}

var tcs = []ingestionTestCase{
	{
		description: "basic employee table",
		createTblSrc: `CREATE TABLE %s (
                          id int NOT NULL,
                          name_target VARCHAR2(120) NOT NULL,
                          salary int NOT NULL,
                          PRIMARY KEY (id))`,
		createTblTgt: `CREATE TABLE %s (
                          "ID" int NOT NULL,
                          "NAME_TARGET" TEXT,
                          "SALARY" int,
                          PRIMARY KEY ("ID"));`,
		queryTblTgt: `SELECT * FROM %s ORDER BY "ID"`,

		execWithRes: []execStmtWithExpectRes{
			{
				description: "INSERT 3 rows",
				execStmts: []string{`BEGIN
FOR v_LoopCounter IN 1..3 LOOP
        INSERT INTO %s (id,name_target,salary)
            VALUES (TO_CHAR(v_LoopCounter),'John Doe',64);
END LOOP;
COMMIT;
END;`},
				expectRes: [][]interface{}{
					{
						int64(1), "John Doe", int64(64),
					},
					{
						int64(2), "John Doe", int64(64),
					},
					{
						int64(3), "John Doe", int64(64),
					},
				},
			},
			{
				description: "UPDATE one row",
				execStmts: []string{`
UPDATE %s 
SET name_target='Pink White'
WHERE ID < 2
`},
				expectRes: [][]interface{}{
					{
						int64(1), "Pink White", int64(64),
					},
					{
						int64(2), "John Doe", int64(64),
					},
					{
						int64(3), "John Doe", int64(64),
					},
				},
			},
			{
				description: "DELETE one row",
				execStmts:   []string{`DELETE FROM %s WHERE id = 2`},
				expectRes: [][]interface{}{
					{
						int64(1), "Pink White", int64(64),
					},
					{
						int64(3), "John Doe", int64(64),
					},
				},
			},
		},
	},
}

// TestStart should be run with following env vars locally on MacOS M1.
// TEST_SOURCE_CONNECT=oracle://sys:password@127.0.0.1:1521/ORCLCDB?sysdba=true
// TEST_TARGET_CONNECT=postgresql://root@localhost:26257/defaultdb?sslmode=disable
// CDC_INTEGRATION=oracle
// -tags="cgo target_all"
func TestStart(t *testing.T) {
	r := require.New(t)
	for _, tc := range tcs {
		t.Run(tc.description, func(t *testing.T) {
			fixture, err := base.NewFixture(t)
			r.NoError(err)

			ctx := fixture.Context

			// Seems a non-zero grace period is necessary for the test to exit.
			defer ctx.Stop(1 * time.Millisecond)

			oraclePool := fixture.SourcePool
			crdbPool := fixture.TargetPool

			// Create the table on the oracle source side.
			srcTblInfo, err := fixture.CreateSourceTable(ctx, tc.createTblSrc)
			r.NoError(err)

			// Create a table with the same table name on the target side.
			tgtTblIdent := ident.NewTable(fixture.TargetSchema.Schema(), srcTblInfo.Name().Table())
			_, err = crdbPool.ExecContext(ctx, fmt.Sprintf(tc.createTblTgt, tgtTblIdent.String()))
			r.NoError(err)

			t.Logf("allocated table %s", srcTblInfo.Name())

			// Need to wait till the SCN is after the create table stmt takes effect.
			// See also https://stackoverflow.com/a/34120192/10400141.
			time.Sleep(4 * time.Second)

			// Acquire the SCN to start.
			var startSCN string
			r.NoError(oraclePool.QueryRowContext(ctx, `SELECT CURRENT_SCN FROM V$DATABASE`).Scan(&startSCN))
			t.Logf("starting SCN: %s", startSCN)

			t.Logf("oraclepool conn str: %s", oraclePool.ConnectionString)

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
				SourceConn:           oraclePool.ConnectionString,
				TargetSchema:         fixture.TargetSchema.Schema(),
				SCN:                  SCN{Val: startSCN},
				SourceSchema:         fixture.SourceSchema.Schema(),
				LogMinerPullInterval: 300 * time.Millisecond,
			}

			// Start the replicator to run in the background.
			_, err = Start(ctx, cfg)
			t.Log("replicator started")
			r.NoError(err)

			for _, execStmtsWithRes := range tc.execWithRes {
				t.Logf("executing for subtest: %s", execStmtsWithRes.description)
				// Run statements within a transaction.
				tx, err := fixture.SourcePool.BeginTx(ctx, nil)
				r.NoError(err)
				for _, stmt := range execStmtsWithRes.execStmts {
					rawName := srcTblInfo.Name().String()
					execStmt := fmt.Sprintf(stmt, rawName)
					_, err = tx.ExecContext(ctx, execStmt)
					r.NoError(errors.Wrapf(err, "executing %s", execStmt))
				}
				r.NoError(tx.Commit())

				// Retry querying the target table and compare the rows
				// with the expected results. We need to retry as there
				// is latency for the relpicator to apply the
				// changefeeds on target.
				retryAttempt, err := retry.NewRetry(retry.Settings{
					InitialBackoff: 1 * time.Second,
					Multiplier:     1,
					MaxRetries:     300,
				})
				r.NoError(err)

				// Retry to query the table result and compare with the
				// post-update expected results.
				r.NoError(retryAttempt.Do(func() error {
					queryStmt := fmt.Sprintf(tc.queryTblTgt, tgtTblIdent.String())
					actualRes, err := retry.QueryWithDynamicRes(ctx, fixture.TargetPool, queryStmt)
					if err != nil {
						return err
					}
					if len(actualRes) == 0 {
						return errors.Errorf("no res yet, retrying")
					}

					if len(execStmtsWithRes.expectRes) != len(actualRes) {
						return errors.Errorf(
							"expected %d res, got %d res",
							len(execStmtsWithRes.expectRes),
							len(actualRes),
						)
					}

					for i := 0; i < len(actualRes); i++ {
						if len(execStmtsWithRes.expectRes[i]) != len(actualRes[i]) {
							return errors.Errorf(
								"expected %d res for row %d, got %d res",
								len(execStmtsWithRes.expectRes[i]),
								i,
								len(actualRes[i]),
							)
						}
						for j := 0; j < len(actualRes[i]); j++ {
							if execStmtsWithRes.expectRes[i][j] != actualRes[i][j] {
								return errors.Errorf(
									"expected res[%d][%d] to be %s, got %s",
									i,
									j,
									execStmtsWithRes.expectRes[i][j],
									actualRes[i][j],
								)
							}
						}
					}

					return nil
				}, func(err error) {
					t.Logf("error reading from target: %s", err.Error())
				}))
			}
		})
	}
}
