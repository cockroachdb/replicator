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
	"github.com/cockroachdb/replicator/internal/sinktest/base"
	"github.com/cockroachdb/replicator/internal/util/retry"
	"github.com/godror/godror"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
)

const (
	sourceConnStr = `oracle://sys:password@127.0.0.1:1521/ORCLCDB?sysdba=true`

	testUserName     = `C##MYADMIN`
	testUserPassword = `MYADMIN`
)

const (
	getSCNStmt = `SELECT CURRENT_SCN FROM V$DATABASE`
)

type ingestionTestCase struct {
	description  string
	createTblSrc string
	createTblTgt string
	insertTblSrc string
	queryTblTgt  string
	updateTblSrc string

	expectPreUpdate  [][]interface{}
	expectPostUpdate [][]interface{}
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
		insertTblSrc: `BEGIN
FOR v_LoopCounter IN 1..3 LOOP
        INSERT INTO employee (id,name_target,salary)
            VALUES (TO_CHAR(v_LoopCounter),'John Doe',64);

END LOOP;
COMMIT;
END;`,
		updateTblSrc: `
UPDATE employee 
SET name_target='Pink White'
WHERE ID < 2
`,
		queryTblTgt: `SELECT * FROM "EMPLOYEE" ORDER BY "ID"`,

		expectPreUpdate: [][]interface{}{
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
		expectPostUpdate: [][]interface{}{
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
}

func TestStart(t *testing.T) {
	r := require.New(t)

	for _, tc := range tcs {
		t.Run(tc.description, func(t *testing.T) {
			ctx := base.ProvideContext(t)
			defer ctx.Stop(200 * time.Millisecond)

			fixture, err := base.NewFixture(t)
			r.NoError(err)

			oraclePool, err := setupOraclePool(ctx, sourceConnStr)
			r.NoError(err)

			_, err = oraclePool.ExecContext(ctx, tc.createTblSrc)
			r.NoError(err)

			var startSCN string
			r.NoError(oraclePool.QueryRowContext(ctx, getSCNStmt).Scan(&startSCN))
			t.Logf("starting SCN: %s", startSCN)

			time.Sleep(4 * time.Second)

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
				TableUser:    testUserName,
			}

			_, err = Start(ctx, cfg)
			t.Logf("replicator started")
			r.NoError(err)

			insertTx, err := oraclePool.BeginTx(ctx, nil)
			r.NoError(err)
			_, err = insertTx.ExecContext(ctx, tc.insertTblSrc)
			r.NoError(err)
			r.NoError(insertTx.Commit())

			retryAttempt, err := retry.NewRetry(retry.Settings{
				InitialBackoff: 1 * time.Second,
				Multiplier:     1,
				MaxRetries:     300,
			})
			r.NoError(err)
			r.NoError(retryAttempt.Do(func() error {
				actualRes, err := QueryWithDynamicRes(ctx, connToRealSchema, tc.queryTblTgt)
				if err != nil {
					return err
				}
				if len(actualRes) == 0 {
					return errors.AssertionFailedf("no res yet, retrying")
				}
				r.Equal(tc.expectPreUpdate, actualRes)
				return nil
			}, func(err error) {
				t.Logf("error reading from target: %s", err.Error())
			}))

			updateTx, err := oraclePool.BeginTx(ctx, nil)
			r.NoError(err)
			_, err = updateTx.ExecContext(ctx, tc.updateTblSrc)
			r.NoError(err)
			r.NoError(updateTx.Commit())

			r.NoError(retryAttempt.Do(func() error {
				actualRes, err := QueryWithDynamicRes(ctx, connToRealSchema, tc.queryTblTgt)
				if err != nil {
					return err
				}
				if len(actualRes) == 0 {
					return errors.AssertionFailedf("no res yet, retrying")
				}

				if len(tc.expectPostUpdate) != len(actualRes) {
					return errors.AssertionFailedf("expected %d res, actual %d res", len(tc.expectPostUpdate), len(actualRes))
				}

				for i := 0; i < len(actualRes); i++ {
					if len(tc.expectPostUpdate[i]) != len(actualRes[i]) {
						return errors.AssertionFailedf("expected %d res for row %d, actual %d res", len(tc.expectPostUpdate[i]), i, len(actualRes[i]))
					}
					for j := 0; j < len(actualRes[i]); j++ {
						if tc.expectPostUpdate[i][j] != actualRes[i][j] {
							return errors.AssertionFailedf("expected %d.%d res to be %s, actual %d", i, j, tc.expectPostUpdate[i][j], actualRes[i][j])
						}
					}
				}

				return nil
			}, func(err error) {
				t.Logf("error reading from target: %s", err.Error())
			}))

		})
	}
}

func QueryWithDynamicRes(
	ctx context.Context, conn *sql.Conn, query string,
) ([][]interface{}, error) {
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	// Get the column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	res := make([][]interface{}, 0)

	for rows.Next() {
		// Create a slice of interface{} to hold each column value
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))

		// Assign the pointers to each interface{} for scanning
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// Scan the row into the value pointers
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		res = append(res, values)
	}

	return res, nil
}

func setupOraclePool(ctx context.Context, sourceConnStr string) (*sql.DB, error) {
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

	const (
		dbName                    = `C##MYADMIN`
		oracleNewUserPassword     = `myadmin`
		OracleUserNotExistErrCode = `ORA-01918`
		OracleMissUserErrCode     = `ORA-01935`
	)

	dropUserQuery := fmt.Sprintf(`DROP USER %s CASCADE`, dbName)
	// We need this ALTER SESSION command to create the user.
	alterSessionQuery := `ALTER SESSION SET "_ORACLE_SCRIPT"=TRUE`
	createUserQuery := fmt.Sprintf("CREATE USER %s IDENTIFIED BY %s DEFAULT TABLESPACE users QUOTA UNLIMITED ON users ACCOUNT UNLOCK", dbName, oracleNewUserPassword)
	grantUserQuery := fmt.Sprintf("GRANT dba TO %s", dbName)
	if _, err := c.ExecContext(ctx, alterSessionQuery); err != nil {
		return nil, errors.Wrapf(err, "failed executing %s", alterSessionQuery)
	}
	if _, err := c.ExecContext(ctx, dropUserQuery); err != nil && !strings.Contains(err.Error(), OracleUserNotExistErrCode) && !strings.Contains(err.Error(), OracleMissUserErrCode) {
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

	params.Username = dbName
	params.Password = godror.NewPassword(oracleNewUserPassword)
	params.IsSysDBA = false

	newDB := sql.OpenDB(godror.NewConnector(params))

	if _, err := newDB.ExecContext(ctx, "alter session set current_schema="+dbName); err != nil {
		return nil, errors.Wrapf(err, "failed setting current schema")
	}

	return newDB, nil
}
