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
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/replicator/internal/sinkprod"
	"github.com/cockroachdb/replicator/internal/sinktest/all"
	"github.com/cockroachdb/replicator/internal/sinktest/base"
	"github.com/cockroachdb/replicator/internal/util/retry"
	"github.com/godror/godror"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
)

const (
	// sql sys/SoupOrSecret@//127.0.0.1:1521/XEPDB1 as sysdba
	sourceConnStr = `oracle://sys:SoupOrSecret@127.0.0.1:1521/XEPDB1?sysdba=true`
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
		createTblSrc: `CREATE TABLE employee (
                          id int NOT NULL,
                          name_target VARCHAR2(120) NOT NULL,
                          salary int NOT NULL,
                          PRIMARY KEY (id))`,
		createTblTgt: `CREATE TABLE "EMPLOYEE" (
                          "ID" int NOT NULL,
                          "NAME_TARGET" TEXT,
                          "SALARY" int,
                          PRIMARY KEY ("ID"));`,
		queryTblTgt: `SELECT * FROM "EMPLOYEE" ORDER BY "ID"`,

		execWithRes: []execStmtWithExpectRes{
			{
				description: "INSERT 3 rows",
				execStmts: []string{`BEGIN
FOR v_LoopCounter IN 1..3 LOOP
        INSERT INTO employee (id,name_target,salary)
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
UPDATE employee 
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
				execStmts:   []string{`DELETE FROM employee WHERE id = 2`},
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

func TestStart(t *testing.T) {
	r := require.New(t)

	for _, tc := range tcs {
		t.Run(tc.description, func(t *testing.T) {
			ctx := base.ProvideContext(t)

			// Seems a non-zero grace period is necessary for the test to exit.
			defer ctx.Stop(1 * time.Millisecond)

			fixture, err := base.NewFixture(t)
			r.NoError(err)

			oraclePool, err := setupOraclePoolForTest(ctx, sourceConnStr)
			r.NoError(err)

			_, err = oraclePool.ExecContext(ctx, tc.createTblSrc)
			r.NoError(err)

			// Need to wait till the SCN is after the create table stmt takes effect.
			// See also https://stackoverflow.com/a/34120192/10400141.
			time.Sleep(4 * time.Second)

			var startSCN string
			r.NoError(oraclePool.QueryRowContext(ctx, `SELECT CURRENT_SCN FROM V$DATABASE`).Scan(&startSCN))
			t.Logf("starting SCN: %s", startSCN)

			crdbPool := fixture.TargetPool
			dbSchema := fixture.TargetSchema.Schema()

			poolCfg, err := pgxpool.ParseConfig(crdbPool.ConnectionString)
			r.NoError(err)
			newDB := dbSchema.Idents(nil)[0].Raw()
			poolCfg.ConnConfig.Database = newDB

			dbToRealSchema := stdlib.OpenDB(*poolCfg.ConnConfig)
			connToRealSchema, err := dbToRealSchema.Conn(ctx)
			r.NoError(err)
			_, err = connToRealSchema.ExecContext(ctx, tc.createTblTgt)
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
				SCN:          startSCN,
				TableUser:    testNewUserName,
			}

			// Start the replicator to run in the background.
			_, err = Start(ctx, cfg)
			t.Log("replicator started")
			r.NoError(err)

			for _, execStmtsWithRes := range tc.execWithRes {
				t.Logf("executing for subtest: %s", execStmtsWithRes.description)
				// Run statements within a transaction.
				tx, err := oraclePool.BeginTx(ctx, nil)
				r.NoError(err)
				for _, stmt := range execStmtsWithRes.execStmts {
					_, err = tx.ExecContext(ctx, stmt)
					r.NoError(err)
				}
				r.NoError(tx.Commit())

				// Retry querying the target table and compare the rows with the expected results.
				// We need to retry as there is latency for the relpicator to apply the changefeeds on
				// target.
				retryAttempt, err := retry.NewRetry(retry.Settings{
					InitialBackoff: 1 * time.Second,
					Multiplier:     1,
					MaxRetries:     300,
				})
				r.NoError(err)

				// Retry to query the table result and compare with the post-update expected results.
				r.NoError(retryAttempt.Do(func() error {
					actualRes, err := queryWithDynamicRes(ctx, connToRealSchema, tc.queryTblTgt)
					if err != nil {
						return err
					}
					if len(actualRes) == 0 {
						return errors.AssertionFailedf("no res yet, retrying")
					}

					if len(execStmtsWithRes.expectRes) != len(actualRes) {
						return errors.AssertionFailedf(
							"expected %d res, got %d res",
							len(execStmtsWithRes.expectRes),
							len(actualRes),
						)
					}

					for i := 0; i < len(actualRes); i++ {
						if len(execStmtsWithRes.expectRes[i]) != len(actualRes[i]) {
							return errors.AssertionFailedf(
								"expected %d res for row %d, got %d res",
								len(execStmtsWithRes.expectRes[i]),
								i,
								len(actualRes[i]),
							)
						}
						for j := 0; j < len(actualRes[i]); j++ {
							if execStmtsWithRes.expectRes[i][j] != actualRes[i][j] {
								return errors.AssertionFailedf(
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

// queryWithDynamicRes runs a query and scan it into a 2D array with {#rows} x {#cols}.
// The size of the 2D arrays is completed determined by the result of the query.
func queryWithDynamicRes(
	ctx context.Context, conn *sql.Conn, query string,
) ([][]interface{}, error) {
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	// Get the number of columns.
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	res := make([][]interface{}, 0)

	for rows.Next() {
		// Create a slice of interface{} to hold each column value.
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))

		// Assign the pointers to each interface{} for scanning.
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// Scan the row into the value pointers.
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		res = append(res, values)
	}

	return res, nil
}

const (
	testNewUserName           = `MYADMIN`
	testNewUserPassword       = `myadmin`
	OracleUserNotExistErrCode = `ORA-01918`
	OracleMissUserErrCode     = `ORA-01935`
)

// setupOraclePoolForTest is to create a new user dedicated for testing, and return the connection pool
// with this new user.
func setupOraclePoolForTest(ctx context.Context, sourceConnStr string) (*sql.DB, error) {
	params, err := godror.ParseDSN(sourceConnStr)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse the source connection string for parameters")
	}
	// Use go's pool, instead of the C library's pool.
	params.StandaloneConnection = true
	// If unset, the driver would otherwise use the local system timezone.
	if params.Timezone == nil {
		params.Timezone = time.UTC
	}

	if ociLibPath := os.Getenv(OCIPathEnvVar); ociLibPath != "" {
		params.LibDir = ociLibPath
	}

	connector := godror.NewConnector(params)
	godrorDB := sql.OpenDB(connector)

	if err := godrorDB.Ping(); err != nil {
		return nil, errors.Wrapf(err, "failed pinging oracle db")
	}

	c, err := godrorDB.Conn(ctx)
	if err != nil {
		return nil, err
	}

	dropUserQuery := fmt.Sprintf(`DROP USER %s CASCADE`, testNewUserName)
	// We need this ALTER SESSION command to create the user.
	alterSessionScriptQuery := `ALTER SESSION SET "_ORACLE_SCRIPT"=TRUE`
	createUserQuery := fmt.Sprintf(`CREATE USER %s IDENTIFIED BY %s DEFAULT TABLESPACE 
users QUOTA UNLIMITED ON users ACCOUNT UNLOCK`, testNewUserName, testNewUserPassword)
	grantUserQuery := fmt.Sprintf("GRANT dba TO %s", testNewUserName)
	alterSessionCurrSchemaQuery := fmt.Sprintf("ALTER SESSION SET current_schema=%s", testNewUserName)

	if _, err := c.ExecContext(ctx, alterSessionScriptQuery); err != nil {
		return nil, errors.Wrapf(err, "failed executing %s", alterSessionScriptQuery)
	}
	if _, err := c.ExecContext(ctx, dropUserQuery); err != nil &&
		!strings.Contains(err.Error(), OracleUserNotExistErrCode) &&
		!strings.Contains(err.Error(), OracleMissUserErrCode) {
		return nil, errors.Wrapf(err, "failed executing %s", dropUserQuery)
	}
	if _, err := c.ExecContext(ctx, createUserQuery); err != nil {
		return nil, errors.Wrapf(err, "failed executing %s", createUserQuery)
	}
	if _, err := c.ExecContext(ctx, grantUserQuery); err != nil {
		return nil, errors.Wrapf(err, "failed executing %s", grantUserQuery)
	}

	if err := c.Close(); err != nil {
		return nil, err
	}

	if err := godrorDB.Close(); err != nil {
		return nil, err
	}

	params.Username = testNewUserName
	params.Password = godror.NewPassword(testNewUserPassword)
	params.IsSysDBA = false

	newDB := sql.OpenDB(godror.NewConnector(params))

	if _, err := newDB.ExecContext(ctx, alterSessionCurrSchemaQuery); err != nil {
		return nil, errors.Wrapf(err, "failed setting current schema")
	}

	return newDB, nil
}
