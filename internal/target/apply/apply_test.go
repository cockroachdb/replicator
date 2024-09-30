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

package apply_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/replicator/internal/sinktest"
	"github.com/cockroachdb/replicator/internal/sinktest/all"
	"github.com/cockroachdb/replicator/internal/sinktest/base"
	table_info "github.com/cockroachdb/replicator/internal/sinktest/base"
	"github.com/cockroachdb/replicator/internal/sinktest/mutations"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/applycfg"
	"github.com/cockroachdb/replicator/internal/util/batches"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/merge"
	"github.com/cockroachdb/replicator/internal/util/retry"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// This test inserts and deletes rows from a trivial table.
func TestApply(t *testing.T) {
	a := assert.New(t)

	fixture, err := all.NewFixture(t)
	if !a.NoError(err) {
		return
	}

	ctx := fixture.Context

	// Note the mixed-case in the json fields. This will ensure that we
	// can insert data in a case-insensitive fashion.
	type Payload struct {
		Pk0        int    `json:"Pk0"`
		Pk1        string `json:"pK1"`
		HasDefault string `json:"has_default,omitempty"`
	}
	var tableSchema string
	switch fixture.TargetPool.Product {
	case types.ProductCockroachDB, types.ProductPostgreSQL:
		// extras could also be JSONB, but validation is easier if we
		// control the exact bytes that come back.
		tableSchema = "CREATE TABLE %s (pk0 INT, pk1 VARCHAR(2048), extras VARCHAR(2048), " +
			"has_default VARCHAR(2048) NOT NULL DEFAULT 'Hello', PRIMARY KEY (pk0,pk1))"
	case types.ProductMariaDB, types.ProductMySQL:
		// MySQL/MariaDB have limitation on the size of the primary key (3072 bytes)
		tableSchema = "CREATE TABLE %s (pk0 INT, pk1 VARCHAR(512), extras VARCHAR(2048), " +
			"has_default VARCHAR(2048) NOT NULL DEFAULT 'Hello', PRIMARY KEY (pk0,pk1))"
	case types.ProductOracle:
		// The lower-case names in the payload will be matched to the
		// upper-case names that exist within the database.
		tableSchema = "CREATE TABLE %s (PK0 INT, PK1 VARCHAR2(4000), EXTRAS VARCHAR2(4000), " +
			"HAS_DEFAULT VARCHAR2(4000) DEFAULT 'Hello' NOT NULL, PRIMARY KEY (PK0,PK1))"
	default:
		a.FailNow("untested product")
	}
	tbl, err := fixture.CreateTargetTable(ctx, tableSchema)
	if !a.NoError(err) {
		return
	}

	// A helper to count the number of rows where has_default = lookFor.
	// https://github.com/cockroachdb/replicator/issues/689
	countHasDefault := func(lookFor string) (ct int, err error) {
		predicate := fmt.Sprintf(`has_default='%s'`, lookFor)
		count, err := table_info.GetRowCountWithPredicateFlex(ctx, fixture.TargetPool, tbl.Name(), fixture.TargetPool.Product, predicate)
		return count, err
	}

	// Use this jumbled name when accessing the API.
	jumbleName := sinktest.JumbleTable(tbl.Name())

	// Helper function to improve readability below.
	apply := fixture.Applier(ctx, jumbleName)

	t.Run("smoke", func(t *testing.T) {
		a := assert.New(t)
		count := 3 * batches.Size()
		adds := make([]types.Mutation, count)
		dels := make([]types.Mutation, count)
		for i := range adds {
			p := Payload{Pk0: i, Pk1: fmt.Sprintf("X%dX", i)}
			if i%2 == 0 {
				p.HasDefault = "World"
			}
			bytes, err := json.Marshal(p)
			a.NoError(err)
			adds[i] = types.Mutation{Data: bytes, Key: bytes}

			bytes, err = json.Marshal([]any{p.Pk0, p.Pk1})
			a.NoError(err)
			dels[i] = types.Mutation{Key: bytes}
		}

		// Verify insertion
		a.NoError(apply(adds))
		ct, err := tbl.RowCount(ctx)
		a.Equal(count, ct)
		a.NoError(err)

		// Ensure SQL DEFAULT value was set.
		ct, err = countHasDefault("Hello")
		a.NoError(err)
		a.Equal(count/2, ct)

		// Ensure SQL DEFAULT value was replaced
		ct, err = countHasDefault("World")
		a.NoError(err)
		a.Equal(count/2, ct)

		// Verify that they can be deleted.
		a.NoError(apply(dels))
		ct, err = tbl.RowCount(ctx)
		a.Equal(0, ct)
		a.NoError(err)
	})

	// Document that if the incoming payload has an explicit null value,
	// we will attempt to send it to the target. This should then fail
	// the NOT NULL constraint.
	t.Run("explicit_null", func(t *testing.T) {
		a := assert.New(t)
		if err := apply([]types.Mutation{
			{
				Data: []byte(`{"pk0":1, "pk1":0, "has_default":null}`),
				Key:  []byte(`[1, 0]`),
			},
		}); a.Error(err) {
			switch fixture.TargetPool.Product {
			case types.ProductCockroachDB, types.ProductPostgreSQL:
				a.ErrorContains(err, "violates not-null constraint (SQLSTATE 23502)")
			case types.ProductMariaDB, types.ProductMySQL:
				a.ErrorContains(err, "1048 (23000)")
			case types.ProductOracle:
				a.ErrorContains(err, "ORA-01400: cannot insert NULL into")
			default:
				a.Fail("unimplemented", err)
			}
		}
	})

	// Verify unexpected incoming column
	t.Run("unexpected", func(t *testing.T) {
		a := assert.New(t)
		if err := apply([]types.Mutation{
			{
				Data: []byte(`{"pk0":1, "pk1":0, "no_good":true}`),
				Key:  []byte(`[1, 0]`),
			},
		}); a.Error(err) {
			a.Contains(err.Error(), "unexpected columns: no_good")
		}
	})

	t.Run("missing_key_upsert", func(t *testing.T) {
		a := assert.New(t)
		if err := apply([]types.Mutation{
			{
				Data: []byte(`{"pk0":1}`),
				Key:  []byte(`[1]`),
			},
		}); a.Error(err) {
			a.Contains(strings.ToLower(err.Error()), "missing pk columns: pk1")
		}
	})

	t.Run("missing_key_delete_too_few", func(t *testing.T) {
		a := assert.New(t)
		if err := apply([]types.Mutation{
			{
				Key: []byte(`[1]`),
			},
		}); a.Error(err) {
			a.Contains(err.Error(), "received 1 expect 2")
		}
	})

	t.Run("missing_key_delete_too_many", func(t *testing.T) {
		a := assert.New(t)
		if err := apply([]types.Mutation{
			{
				Key: []byte(`[1, 2, 3]`),
			},
		}); a.Error(err) {
			a.Contains(err.Error(), "received 3 expect 2")
		}
	})

	// Verify that unknown columns can be saved.
	t.Run("extras", func(t *testing.T) {
		a := assert.New(t)
		cfg := applycfg.NewConfig()
		cfg.Extras = ident.New("extras")
		a.NoError(fixture.Configs.Set(jumbleName, cfg))

		// The config update is async, so we may need to try again.
		for {
			err := apply([]types.Mutation{
				{
					Data: []byte(`{"pk0":1, "pk1":"0", "heretofore":"unseen", "are":"OK"}`),
					Key:  []byte(`[1, "0"]`),
				},
				{
					Data: []byte(`{"pk0":2, "pk1":"0", "check":"multiple", "mutations":"work"}`),
					Key:  []byte(`[2, "0"]`),
				},
			})
			if err == nil {
				break
			}
			if strings.Contains(err.Error(), "unexpected columns") {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			a.NoError(err)
			return
		}

		var extrasQ string
		switch fixture.TargetPool.Product {
		case types.ProductCockroachDB, types.ProductPostgreSQL:
			extrasQ = fmt.Sprintf("SELECT extras FROM %s WHERE pk0=$1 AND pk1=$2", tbl.Name())
		case types.ProductMariaDB, types.ProductMySQL:
			extrasQ = fmt.Sprintf("SELECT extras FROM %s WHERE pk0=? AND pk1=?", tbl.Name())
		case types.ProductOracle:
			extrasQ = fmt.Sprintf(`SELECT extras FROM %s WHERE pk0=:p1 AND pk1=:p2`, tbl.Name())
		default:
			a.Fail("unimplemented", fixture.TargetPool.Product)
		}
		var extrasRaw string
		a.NoError(fixture.TargetPool.QueryRowContext(ctx, extrasQ, 1, "0").Scan(&extrasRaw))
		a.Equal(`{"are":"OK","heretofore":"unseen"}`, extrasRaw)

		a.NoError(fixture.TargetPool.QueryRowContext(ctx, extrasQ, 2, "0").Scan(&extrasRaw))
		a.Equal(`{"check":"multiple","mutations":"work"}`, extrasRaw)

		a.NoError(fixture.Configs.Set(jumbleName, nil))
	})

	// This sub-test uses the apply from concurrent goroutines to allow
	// the race checker to help us.
	t.Run("concurrent_apply", func(t *testing.T) {
		r := require.New(t)
		const batchSize = 1_000
		const numUpserts = 100_000
		const numWorkers = 10
		const upsertsPerWorker = numUpserts / numWorkers

		// We're sending a workload to Oracle without using the lockset
		// scheduler. This can cause Oracle's deadlock detector to fire
		// because multiple merge statements may indeed have overlapping
		// key sets.
		distinctKeys := 10_000
		if fixture.TargetPool.Product == types.ProductOracle {
			distinctKeys = numUpserts
		}

		// Clean up data.
		_, err := fixture.TargetPool.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE 1=1", tbl.Name()))
		r.NoError(err)
		ct, err := tbl.RowCount(ctx)
		r.NoError(err)
		r.Equal(0, ct)

		var nextKey atomic.Int32
		eg, egCtx := errgroup.WithContext(ctx)
		for i := 0; i < numWorkers; i++ {
			eg.Go(func() error {
				remaining := upsertsPerWorker
				for remaining > 0 && egCtx.Err() == nil {
					currentBatchSize := remaining
					if currentBatchSize > batchSize {
						currentBatchSize = batchSize
					}
					muts := make([]types.Mutation, currentBatchSize)
					for i := range muts {
						key := int(nextKey.Add(1)) % distinctKeys
						p := Payload{Pk0: int(key), Pk1: fmt.Sprintf("X%dX", key)}
						dataBytes, err := json.Marshal(p)
						a.NoError(err)
						keyBytes, err := json.Marshal([]any{p.Pk0, p.Pk1})
						a.NoError(err)

						muts[i] = types.Mutation{Data: dataBytes, Key: keyBytes}
					}
					if err := retry.Retry(egCtx, fixture.TargetPool, func(_ context.Context) error {
						return apply(muts)
					}); err != nil {
						return err
					}
					remaining -= batchSize
				}
				return nil
			})
		}
		r.NoError(eg.Wait())

		ct, err = tbl.RowCount(ctx)
		r.NoError(err)
		r.Equal(distinctKeys, ct)
	})
}

type fakeAcceptor struct{}

var _ types.TableAcceptor = (*fakeAcceptor)(nil)

func (f *fakeAcceptor) AcceptTableBatch(
	context.Context, *types.TableBatch, *types.AcceptOptions,
) error {
	return nil
}

type dataTypeTestCase struct {
	name       string // (Required) Test case name.
	sourceType string // (Optional) Override columnType in source db.
	columnType string // (Required) Type in the source and target database.
	sqlValue   string // (Required) Expression that the source database can turn into JSON.
	expectJSON string // (Optional) JSON expression to expect. Defaults to toJSON(sqlValue).
	indexable  bool   // (Optional) Use the type as a primary key column.
	readBackQ  string // (Optional) Ad-hoc query to get a string value from the target database.

	// (Optional) Allow the test to be skipped
	skip func(fixture *all.Fixture) bool
}

var (
	mariaDBDataTypeTests = []dataTypeTestCase{
		// MariaDB types: https://mariadb.com/kb/en/data-types/
		{name: `bigint_null`, columnType: `BIGINT`},
		{name: `bigint`, columnType: `BIGINT`, sqlValue: `9223372036854775807`},
		{name: `binary_null`, sourceType: `bytes`, columnType: `BINARY(6)`},
		{
			name:       `binary`,
			sourceType: `bytes`,
			columnType: `BINARY(6)`,
			sqlValue:   `a1b2c3`,
			indexable:  true,
			expectJSON: `"a1b2c3"`,
			readBackQ:  `SELECT val FROM %s`,
		},
		{name: `bit_null`, columnType: `BIT(8)`},
		{
			name:       `bit`,
			sourceType: `varbit`,
			columnType: `BIT(8)`,
			sqlValue:   `10010101`,
			expectJSON: `"10010101"`,
			readBackQ:  `SELECT bin(val) FROM %s`,
		},
		{name: `blob_null`, sourceType: `bytes`, columnType: `BLOB(100)`},
		{
			name:       `blob`,
			sourceType: `bytes`,
			columnType: `BLOB(100)`,
			sqlValue:   `a1b2c3`,
			expectJSON: `"a1b2c3"`,
			readBackQ:  `SELECT val FROM %s`,
		},
		{name: `char_null`, columnType: `CHAR(255)`},
		{name: `char`, columnType: `CHAR(255)`, sqlValue: `a1b2c3`, indexable: true},
		{name: `date_null`, columnType: `DATE`},
		{name: `date`, columnType: `DATE`, sqlValue: `2016-01-25`, indexable: true},
		{name: `datetime_null`, sourceType: `timestamp`, columnType: `DATETIME`},
		{
			name:       `datetime`,
			sourceType: `timestamp`,
			columnType: `DATETIME`,
			sqlValue:   `2016-01-25 01:01:00`,
			expectJSON: `"2016-01-25 01:01:00"`,
			indexable:  true,
		},
		{name: `decimal_eng_6,0`, columnType: `DECIMAL(6,0)`, sqlValue: `4e+2`, indexable: true, expectJSON: "400"},
		{name: `decimal_eng_6,2`, columnType: `DECIMAL(6,2)`, sqlValue: `4.98765e+2`, indexable: true, expectJSON: "498.77"},
		{name: `decimal_null`, columnType: `DECIMAL`},
		{name: `decimal`, columnType: `DECIMAL(10,4)`, sqlValue: `1.2345`, indexable: true},
		{name: `double_null`, sourceType: `float`, columnType: `DOUBLE`},
		{name: `double`, sourceType: `float`, columnType: `DOUBLE`, sqlValue: `1.2345`, indexable: true},
		{name: `enum_null`, sourceType: `string`, columnType: `ENUM('a','b','c')`},
		{name: `enum`, sourceType: `string`, columnType: `ENUM('a','b','c')`, sqlValue: `a`, indexable: true},
		{name: `float_null`, columnType: `FLOAT`},
		// This fails with lower precision:
		// select json_array(cast(1.2345 as float)) => 1.2345000505447388
		{name: `float`, sourceType: `float`, columnType: `FLOAT(25)`, sqlValue: `1.2345`},
		{
			name:       `geography`,
			sourceType: `GEOGRAPHY`,
			columnType: `GEOMETRY`,
			sqlValue:   `0101000020E6100000000000000000F03F0000000000000040`,
			expectJSON: `"POINT(1 2)"`,
			readBackQ:  `select ST_AsText(val) from %s`,
		},
		{name: `geometry`,
			columnType: `GEOMETRY`,
			sqlValue:   `010100000075029A081B9A5DC0F085C954C1F84040`,
			expectJSON: `"POINT(-118.4079 33.9434)"`,
			readBackQ:  `select ST_AsText(val) from %s`,
		},
		{name: `inet`, sourceType: `INET`, columnType: `INET4`, sqlValue: `192.168.0.1`, indexable: true},
		{name: `inet_null`, sourceType: `INET`, columnType: `INET4`},
		{name: `int_null`, columnType: `INT`},
		{name: `int`, columnType: `INT`, sqlValue: `2147483647`},
		{name: `integer_null`, columnType: `INTEGER`},
		{name: `integer`, columnType: `INTEGER`, sqlValue: `2147483647`},
		{name: `jsonb_null`, sourceType: `jsonb`, columnType: `JSON`},
		{
			name:       `json`,
			sourceType: `jsonb`,
			columnType: `JSON`,
			sqlValue: `
			{
				"string": "Lola",
				"bool": true,
				"number": 547,
				"float": 123.456,
				"array": [
					"lola",
					true,
					547,
					123.456,
					[
						"lola",
						true,
						547,
						123.456
					],
					{
						"string": "Lola",
						"bool": true,
						"number": 547,
						"float": 123.456,
						"array": [
							"lola",
							true,
							547,
							123.456,
							[
								"lola",
								true,
								547,
								123.456
							]
						]
					}
				],
				"map": {
					"string": "Lola",
					"bool": true,
					"number": 547,
					"float": 123.456,
					"array": [
						"lola",
						true,
						547,
						123.456,
						[
							"lola",
							true,
							547,
							123.456
						],
						{
							"string": "Lola",
							"bool": true,
							"number": 547,
							"float": 123.456,
							"array": [
								"lola",
								true,
								547,
								123.456,
								[
									"lola",
									true,
									547,
									123.456
								]
							]
						}
					]
				}
			}
			`,
		},
		{name: `longblob_null`, sourceType: `bytes`, columnType: `LONGBLOB`},
		{
			name:       `longblob`,
			sourceType: `bytes`,
			columnType: `LONGBLOB`,
			sqlValue:   `a1b2c3`,
			expectJSON: `"a1b2c3"`,
			readBackQ:  `SELECT val FROM %s`,
		},
		{name: `longtext_null`, sourceType: `string`, columnType: `LONGTEXT`},
		{name: `longtext`, sourceType: `string`, columnType: `LONGTEXT`, sqlValue: `a1b2c3`},
		{name: `mediumblob_null`, sourceType: `bytes`, columnType: `MEDIUMBLOB`},
		{
			name:       `mediumblob`,
			sourceType: `bytes`,
			columnType: `MEDIUMBLOB`,
			sqlValue:   `a1b2c3`,
			expectJSON: `"a1b2c3"`,
			readBackQ:  `SELECT val FROM %s`,
		},
		{name: `mediumint_null`, sourceType: `int4`, columnType: `MEDIUMINT`},
		{name: `mediumint`, sourceType: `int4`, columnType: `MEDIUMINT`, sqlValue: `8388607`},
		{name: `mediumtext_null`, sourceType: `string`, columnType: `MEDIUMTEXT`},
		{name: `mediumtext`, sourceType: `string`, columnType: `MEDIUMTEXT`, sqlValue: `a1b2c3`},
		{name: `mumeric_null`, columnType: `DECIMAL`},
		{name: `numeric`, columnType: `DECIMAL(10,4)`, sqlValue: `1.2345`, indexable: true},
		{name: `smallint_null`, sourceType: `int2`, columnType: `SMALLINT`},
		{name: `smallint`, sourceType: `int2`, columnType: `SMALLINT`, sqlValue: `32767`},
		{name: `set_null`, sourceType: `string`, columnType: `SET('a','b','c')`},
		{name: `set`, sourceType: `string`, columnType: `SET('a','b','c')`, sqlValue: `a,b`, indexable: true},
		{name: `text_null`, sourceType: `string`, columnType: `TEXT(100)`},
		{name: `text`, sourceType: `string`, columnType: `TEXT(100)`, sqlValue: `a1b2c3`},
		{name: `time_null`, columnType: `TIME`},
		{name: `time`, columnType: `TIME(6)`, sqlValue: `01:23:45.123456`, indexable: true},
		{name: `timestamp_null`, columnType: `TIMESTAMP`},
		{
			name:       `timestamp`,
			columnType: `TIMESTAMP`,
			sqlValue:   `2016-01-25 10:10:10`,
			expectJSON: `"2016-01-25 10:10:10"`,
			indexable:  true,
		},
		{name: `tinyblob_null`, sourceType: `bytes`, columnType: `TINYBLOB`},
		{
			name:       `tinyblob`,
			sourceType: `bytes`,
			columnType: `TINYBLOB`,
			sqlValue:   `ab`,
			expectJSON: `"ab"`,
			readBackQ:  `SELECT val FROM %s`,
		},
		{name: `tinyint_null`, sourceType: `int2`, columnType: `TINYINT`},
		{name: `tinyint`, sourceType: `int2`, columnType: `TINYINT`, sqlValue: `127`},
		{name: `tinytext_null`, sourceType: `string`, columnType: `TINYTEXT`},
		{name: `tinytext`, sourceType: `string`, columnType: `TINYTEXT`, sqlValue: `a1b2c3`},
		{name: `varbinary_null`, sourceType: `bytes`, columnType: `VARBINARY(255)`},
		{
			name:       `varbinary`,
			sourceType: `bytes`,
			columnType: `VARBINARY(255)`,
			sqlValue:   `a1b2c3`,
			expectJSON: `"a1b2c3"`,
			readBackQ:  `SELECT val FROM %s`,
		},
		{name: `varchar_null`, columnType: `VARCHAR(255)`},
		{name: `varchar`, columnType: `VARCHAR(255)`, sqlValue: `a1b2c3`, indexable: true},
		{name: `year_null`, sourceType: `string`, columnType: `YEAR`},
		{name: `year`, sourceType: `string`, columnType: `YEAR`, sqlValue: `2016`, expectJSON: `2016`, indexable: true},
	}
	myDataTypeTests = []dataTypeTestCase{
		// MySQL data types: https://dev.mysql.com/doc/refman/8.0/en/data-types.html
		// 11.1.2 Integer Types (Exact Value) - INTEGER, INT, SMALLINT, TINYINT, MEDIUMINT, BIGINT
		// 11.1.3 Fixed-Point Types (Exact Value) - DECIMAL, NUMERIC
		// 11.1.4 Floating-Point Types (Approximate Value) - FLOAT, DOUBLE
		// 11.1.5 Bit-Value Type - BIT
		// 11.2.2 The DATE, DATETIME, and TIMESTAMP Types
		// 11.2.3 The TIME Type
		// 11.2.4 The YEAR Type
		// 11.3.2 The CHAR and VARCHAR Types
		// 11.3.3 The BINARY and VARBINARY Types
		// 11.3.4 The BLOB and TEXT Types
		// 11.3.5 The ENUM Type
		// 11.3.6 The SET Type
		// 11.4 Spatial Data Types
		// 11.5 The JSON Data Type

		{name: `bigint_null`, columnType: `BIGINT`},
		{name: `bigint`, columnType: `BIGINT`, sqlValue: `9223372036854775807`},
		{name: `binary_null`, sourceType: `bytes`, columnType: `BINARY(6)`},
		{
			name:       `binary`,
			sourceType: `bytes`,
			columnType: `BINARY(6)`,
			sqlValue:   `a1b2c3`,
			indexable:  true,
			expectJSON: `"a1b2c3"`,
			readBackQ:  `SELECT val FROM %s`,
		},
		{name: `bit_null`, columnType: `BIT(8)`},
		{
			name:       `bit`,
			sourceType: `varbit`,
			columnType: `BIT(8)`,
			sqlValue:   `10010101`,
			expectJSON: `"10010101"`,
			readBackQ:  `SELECT bin(val) FROM %s`,
		},
		{name: `blob_null`, sourceType: `bytes`, columnType: `BLOB(100)`},
		{
			name:       `blob`,
			sourceType: `bytes`,
			columnType: `BLOB(100)`,
			sqlValue:   `a1b2c3`,
			expectJSON: `"a1b2c3"`,
			readBackQ:  `SELECT val FROM %s`,
		},
		{name: `char_null`, columnType: `CHAR(255)`},
		{name: `char`, columnType: `CHAR(255)`, sqlValue: `a1b2c3`, indexable: true},
		{name: `date_null`, columnType: `DATE`},
		{name: `date`, columnType: `DATE`, sqlValue: `2016-01-25`, indexable: true},
		{name: `datetime_null`, sourceType: `timestamp`, columnType: `DATETIME`},
		{
			name:       `datetime`,
			sourceType: `timestamp`,
			columnType: `DATETIME`,
			sqlValue:   `2016-01-25 01:01:00`,
			expectJSON: `"2016-01-25 01:01:00.000000"`,
			indexable:  true,
		},
		{name: `decimal_eng_6,0`, columnType: `DECIMAL(6,0)`, sqlValue: `4e+2`, indexable: true, expectJSON: "400"},
		{name: `decimal_eng_6,2`, columnType: `DECIMAL(6,2)`, sqlValue: `4.98765e+2`, indexable: true, expectJSON: "498.77"},
		{name: `decimal_null`, columnType: `DECIMAL`},
		{name: `decimal`, columnType: `DECIMAL(10,4)`, sqlValue: `1.2345`, indexable: true},
		{name: `double_null`, sourceType: `float`, columnType: `DOUBLE`},
		{name: `double`, sourceType: `float`, columnType: `DOUBLE`, sqlValue: `1.2345`, indexable: true},
		{name: `enum_null`, sourceType: `string`, columnType: `ENUM('a','b','c')`},
		{name: `enum`, sourceType: `string`, columnType: `ENUM('a','b','c')`, sqlValue: `a`, indexable: true},
		{name: `float_null`, columnType: `FLOAT`},
		// This fails with lower precision:
		// select json_array(cast(1.2345 as float)) => 1.2345000505447388
		{name: `float`, sourceType: `float`, columnType: `FLOAT(25)`, sqlValue: `1.2345`},
		{
			name:       `geography`,
			sourceType: `GEOGRAPHY`,
			columnType: `GEOMETRY`,
			sqlValue:   `0101000020E6100000000000000000F03F0000000000000040`,
			expectJSON: `{"coordinates":[1.0,2.0],"type":"Point"}`,
		},
		{name: `geometry`, columnType: `GEOMETRY`, sqlValue: `010100000075029A081B9A5DC0F085C954C1F84040`},
		{name: `int_null`, columnType: `INT`},
		{name: `int`, columnType: `INT`, sqlValue: `2147483647`},
		{name: `integer_null`, columnType: `INTEGER`},
		{name: `integer`, columnType: `INTEGER`, sqlValue: `2147483647`},
		{name: `jsonb_null`, sourceType: `jsonb`, columnType: `JSON`},
		{
			name:       `json`,
			sourceType: `jsonb`,
			columnType: `JSON`,
			sqlValue: `
			{
				"string": "Lola",
				"bool": true,
				"number": 547,
				"float": 123.456,
				"array": [
					"lola",
					true,
					547,
					123.456,
					[
						"lola",
						true,
						547,
						123.456
					],
					{
						"string": "Lola",
						"bool": true,
						"number": 547,
						"float": 123.456,
						"array": [
							"lola",
							true,
							547,
							123.456,
							[
								"lola",
								true,
								547,
								123.456
							]
						]
					}
				],
				"map": {
					"string": "Lola",
					"bool": true,
					"number": 547,
					"float": 123.456,
					"array": [
						"lola",
						true,
						547,
						123.456,
						[
							"lola",
							true,
							547,
							123.456
						],
						{
							"string": "Lola",
							"bool": true,
							"number": 547,
							"float": 123.456,
							"array": [
								"lola",
								true,
								547,
								123.456,
								[
									"lola",
									true,
									547,
									123.456
								]
							]
						}
					]
				}
			}
			`,
		},
		{name: `longblob_null`, sourceType: `bytes`, columnType: `LONGBLOB`},
		{
			name:       `longblob`,
			sourceType: `bytes`,
			columnType: `LONGBLOB`,
			sqlValue:   `a1b2c3`,
			expectJSON: `"a1b2c3"`,
			readBackQ:  `SELECT val FROM %s`,
		},
		{name: `longtext_null`, sourceType: `string`, columnType: `LONGTEXT`},
		{name: `longtext`, sourceType: `string`, columnType: `LONGTEXT`, sqlValue: `a1b2c3`},
		{name: `mediumblob_null`, sourceType: `bytes`, columnType: `MEDIUMBLOB`},
		{
			name:       `mediumblob`,
			sourceType: `bytes`,
			columnType: `MEDIUMBLOB`,
			sqlValue:   `a1b2c3`,
			expectJSON: `"a1b2c3"`,
			readBackQ:  `SELECT val FROM %s`,
		},
		{name: `mediumint_null`, sourceType: `int4`, columnType: `MEDIUMINT`},
		{name: `mediumint`, sourceType: `int4`, columnType: `MEDIUMINT`, sqlValue: `8388607`},
		{name: `mediumtext_null`, sourceType: `string`, columnType: `MEDIUMTEXT`},
		{name: `mediumtext`, sourceType: `string`, columnType: `MEDIUMTEXT`, sqlValue: `a1b2c3`},
		{name: `mumeric_null`, columnType: `DECIMAL`},
		{name: `numeric`, columnType: `DECIMAL(10,4)`, sqlValue: `1.2345`, indexable: true},
		{name: `smallint_null`, sourceType: `int2`, columnType: `SMALLINT`},
		{name: `smallint`, sourceType: `int2`, columnType: `SMALLINT`, sqlValue: `32767`},
		{name: `set_null`, sourceType: `string`, columnType: `SET('a','b','c')`},
		{name: `set`, sourceType: `string`, columnType: `SET('a','b','c')`, sqlValue: `a,b`, indexable: true},
		{name: `text_null`, sourceType: `string`, columnType: `TEXT(100)`},
		{name: `text`, sourceType: `string`, columnType: `TEXT(100)`, sqlValue: `a1b2c3`},
		{name: `time_null`, columnType: `TIME`},
		{name: `time`, columnType: `TIME(6)`, sqlValue: `01:23:45.123456`, indexable: true},
		{name: `timestamp_null`, columnType: `TIMESTAMP`},
		{
			name:       `timestamp`,
			columnType: `TIMESTAMP`,
			sqlValue:   `2016-01-25 10:10:10`,
			expectJSON: `"2016-01-25 10:10:10.000000"`,
			indexable:  true,
		},
		{name: `tinyblob_null`, sourceType: `bytes`, columnType: `TINYBLOB`},
		{
			name:       `tinyblob`,
			sourceType: `bytes`,
			columnType: `TINYBLOB`,
			sqlValue:   `ab`,
			expectJSON: `"ab"`,
			readBackQ:  `SELECT val FROM %s`,
		},
		{name: `tinyint_null`, sourceType: `int2`, columnType: `TINYINT`},
		{name: `tinyint`, sourceType: `int2`, columnType: `TINYINT`, sqlValue: `127`},
		{name: `tinytext_null`, sourceType: `string`, columnType: `TINYTEXT`},
		{name: `tinytext`, sourceType: `string`, columnType: `TINYTEXT`, sqlValue: `a1b2c3`},
		{name: `varbinary_null`, sourceType: `bytes`, columnType: `VARBINARY(255)`},
		{
			name:       `varbinary`,
			sourceType: `bytes`,
			columnType: `VARBINARY(255)`,
			sqlValue:   `a1b2c3`,
			expectJSON: `"a1b2c3"`,
			readBackQ:  `SELECT val FROM %s`,
		},
		{name: `varchar_null`, columnType: `VARCHAR(255)`},
		{name: `varchar`, columnType: `VARCHAR(255)`, sqlValue: `a1b2c3`, indexable: true},
		{name: `year_null`, sourceType: `string`, columnType: `YEAR`},
		{name: `year`, sourceType: `string`, columnType: `YEAR`, sqlValue: `2016`, expectJSON: `2016`, indexable: true},
	}
	oraDataTypeTests = []dataTypeTestCase{
		{
			// Dates are returned with a midnight time.
			name:       `date`,
			columnType: `DATE`,
			sqlValue:   `2016-01-25`,
			expectJSON: `"2016-01-25T00:00:00"`,
			indexable:  true,
		},
		{name: `date_null`, columnType: `DATE`},
		{
			name:       `decimal_to_number`,
			sourceType: `DECIMAL(6,3)`,
			sqlValue:   `123.456`,
			columnType: `NUMBER(6,3)`,
			indexable:  true,
		},
		{
			name:       `varchar_to_char_1_byte`,
			sourceType: `STRING`,
			sqlValue:   `Y`,
			columnType: `CHAR(1 BYTE)`,
			indexable:  true,
		},
		{
			name:       `varchar_to_char_1_char`,
			sourceType: `STRING`,
			sqlValue:   `Y`,
			columnType: `CHAR(1 CHAR)`,
			indexable:  true,
		},
		{name: `int`, columnType: `INT`, sqlValue: `12345`, indexable: true},
		{name: `int_null`, columnType: `INT`},
		{name: `string`, columnType: `VARCHAR(4000)`, sqlValue: `a1b2c3`, indexable: true},
		{name: `string_null`, columnType: `VARCHAR(4000)`},
		{name: `string_escape`, columnType: `VARCHAR(4000)`, sqlValue: `a1\b/2?c"3`, indexable: true},
		{
			name:       `timestamp`,
			columnType: `TIMESTAMP`, // Defaults to TIMESTAMP(6)
			sqlValue:   `2016-01-25 10:10:10.123`,
			expectJSON: `"2016-01-25T10:10:10.123000"`,
			indexable:  true},
		{name: `timestamp_null`, columnType: `TIMESTAMP`},
		{
			// ORA-02329: column of datatype TIME/TIMESTAMP WITH TIME ZONE cannot be unique or a primary key
			name:       `timestamptz`,
			columnType: `TIMESTAMP WITH TIME ZONE`, // Defaults to TIMESTAMP(6)
			sqlValue:   `2016-01-25 10:10:10.123-05:00`,
			expectJSON: `"2016-01-25T15:10:10.123000Z"`,
		},
		{name: `timestamptz_null`, columnType: `TIMESTAMP WITH TIME ZONE`},
		{
			name:       `uuid_to_raw`,
			sourceType: `UUID`,
			columnType: `RAW(16)`,
			sqlValue:   `01C2A76E-DD87-492E-B129-F6E9ACD89556`,
			expectJSON: `"01C2A76EDD87492EB129F6E9ACD89556"`,
		},
	}

	pgDataTypeTests = []dataTypeTestCase{
		{name: `string_array`, columnType: `STRING[]`, sqlValue: `{"sky","road","car"}`},
		{name: `string_array_null`, columnType: `STRING[]`},
		{name: `int_array`, columnType: `INT[]`, sqlValue: `{1,2,3}`},
		{name: `int_array_null`, columnType: `INT[]`},
		{name: `serial_array`, columnType: `SERIAL[]`, sqlValue: `{148591304110702593,148591304110702594,148591304110702595}`},
		{name: `serial_array_null`, columnType: `SERIAL[]`},
		{name: `bit`, columnType: `VARBIT`, sqlValue: `10010101`, indexable: true},
		{name: `bit_null`, columnType: `VARBIT`},
		{name: `bool`, columnType: `BOOL`, sqlValue: `true`, indexable: true},
		{name: `bool_array`, columnType: `BOOL[]`, sqlValue: `{true, false, true}`},
		{name: `bool_null`, columnType: `BOOL`},
		{name: `bytea`, columnType: `BYTEA`, sqlValue: `b'\141\061\142\062\143\063'`, indexable: true},
		{name: `bytes`, columnType: `BYTES`, sqlValue: `b'\141\061\142\062\143\063'`, indexable: true},
		{name: `collate`, columnType: `STRING COLLATE de`, sqlValue: `'a1b2c3' COLLATE de`, indexable: true},
		{name: `collate_null`, columnType: `STRING COLLATE de`},
		{name: `date`, columnType: `DATE`, sqlValue: `2016-01-25`, indexable: true},
		{name: `date_null`, columnType: `DATE`},
		{name: `decimal`, columnType: `DECIMAL`, sqlValue: `1.2345`, indexable: true},
		{name: `decimal_eng_6,0`, columnType: `DECIMAL(6,0)`, sqlValue: `4e+2`, indexable: true, expectJSON: "400"},
		{
			name:       `decimal_eng_6,2`,
			columnType: `DECIMAL(6,2)`,
			sqlValue:   `4.98765e+2`,
			indexable:  true,
			expectJSON: "498.77",
			skip: func(f *all.Fixture) bool {
				// The delete test fails; the rounding seems to break
				// down somewhere. Not worth fixing since v21 is so old
				// at this point.
				return f.TargetPool.Product == types.ProductCockroachDB &&
					strings.Contains(f.TargetPool.Version, "v21.")
			},
		},
		{
			// Bigger than int64
			name: `decimal_eng_50,2`, columnType: `DECIMAL(600,2)`, sqlValue: `4e+50`, indexable: true,
			expectJSON: "400000000000000000000000000000000000000000000000000.00",
		},
		{name: `decimal_null`, columnType: `DECIMAL`},
		{name: `float`, columnType: `FLOAT`, sqlValue: `1.2345`, indexable: true},
		{name: `float_null`, columnType: `FLOAT`},
		{name: `geography`, columnType: `GEOGRAPHY`, sqlValue: `0101000020E6100000000000000000F03F0000000000000040`},
		{name: `geometry`, columnType: `GEOMETRY`, sqlValue: `010100000075029A081B9A5DC0F085C954C1F84040`},
		{name: `inet`, columnType: `INET`, sqlValue: `192.168.0.1`, indexable: true},
		{name: `inet_null`, columnType: `INET`},
		{name: `int`, columnType: `INT`, sqlValue: `12345`, indexable: true},
		{name: `int_null`, columnType: `INT`},
		{name: `interval`, columnType: `INTERVAL`, sqlValue: `2h30m30s`, indexable: true},
		{name: `interval_null`, columnType: `INTERVAL`},
		{
			name:       `jsonb`,
			columnType: `JSONB`,
			sqlValue: `
			{
				"string": "Lola",
				"bool": true,
				"number": 547,
				"float": 123.456,
				"array": [
					"lola",
					true,
					547,
					123.456,
					[
						"lola",
						true,
						547,
						123.456
					],
					{
						"string": "Lola",
						"bool": true,
						"number": 547,
						"float": 123.456,
						"array": [
							"lola",
							true,
							547,
							123.456,
							[
								"lola",
								true,
								547,
								123.456
							]
						]
					}
				],
				"map": {
					"string": "Lola",
					"bool": true,
					"number": 547,
					"float": 123.456,
					"array": [
						"lola",
						true,
						547,
						123.456,
						[
							"lola",
							true,
							547,
							123.456
						],
						{
							"string": "Lola",
							"bool": true,
							"number": 547,
							"float": 123.456,
							"array": [
								"lola",
								true,
								547,
								123.456,
								[
									"lola",
									true,
									547,
									123.456
								]
							]
						}
					]
				}
			}
			`,
		},
		{name: `jsonb_null`, columnType: `JSONB`},
		{name: `serial`, columnType: `SERIAL8`, sqlValue: `148591304110702593`, indexable: true},
		// serial cannot be null
		{name: `string`, columnType: `STRING`, sqlValue: `a1b2c3`, indexable: true},
		{name: `string_null`, columnType: `STRING`},
		{name: `string_escape`, columnType: `STRING`, sqlValue: `a1\b/2?c"3`, indexable: true},
		{name: `time`, columnType: `TIME`, sqlValue: `01:23:45.123456`, indexable: true},
		{name: `time_null`, columnType: `TIME`},
		{name: `timestamp`, columnType: `TIMESTAMP`, sqlValue: `2016-01-25 10:10:10`, indexable: true},
		{name: `timestamp_null`, columnType: `TIMESTAMP`},
		// timestamptz is a bit different in PG and CRDB, adding a product specific test case below.
		{name: `timestamptz_null`, columnType: `TIMESTAMPTZ`},
		{name: `uuid`, columnType: `UUID`, sqlValue: `7f9c24e8-3b12-4fef-91e0-56a2d5a246ec`, indexable: true},
		{name: `uuid_null`, columnType: `UUID`},
		{name: `varchar`, columnType: `VARCHAR(2048)`, sqlValue: `a1b2c3`, indexable: true},
		{name: `varchar_null`, columnType: `VARCHAR(2048)`},
		{name: `varchar_array`, columnType: `VARCHAR(2048)[]`, sqlValue: `{"sky","road","car"}`},
		{name: `varchar_array_null`, columnType: `VARCHAR(2048)[]`},
	}
)

// This is a smoke test  to ensure that
// all supported data types can be applied. It works by creating
// a test table for each type and using CRDB's built-in to_jsonb()
// function to create a payload.
func TestAllDataTypes(t *testing.T) {

	var expectArrayWrapper bool
	var readBackQ string
	var testcases []dataTypeTestCase
	{
		fixture, err := all.NewFixture(t)
		require.NoError(t, err)

		switch fixture.TargetPool.Product {
		case types.ProductCockroachDB:
			// timestamptz CRDB specific
			testcases = append(pgDataTypeTests,
				dataTypeTestCase{
					name:       `timestamptz`,
					columnType: `TIMESTAMPTZ`,
					sqlValue:   `2016-01-25 10:10:10-05:00`,
					indexable:  true})
			readBackQ = "SELECT COALESCE(to_json(val)::VARCHAR(2048), 'null') FROM %s"
		case types.ProductMariaDB:
			testcases = mariaDBDataTypeTests
			readBackQ = "SELECT COALESCE(json_extract(json_array(val),'$[0]'), 'null') FROM %s"
		case types.ProductMySQL:
			testcases = myDataTypeTests
			readBackQ = "SELECT COALESCE(json_extract(json_array(val),'$[0]'), 'null') FROM %s"
		case types.ProductOracle:
			testcases = oraDataTypeTests
			// JSON_QUERY in older versions refuses to return raw scalars.
			readBackQ = "SELECT JSON_QUERY(JSON_ARRAY(val NULL ON NULL), '$[0]' WITH ARRAY WRAPPER) FROM %s"
			expectArrayWrapper = true
		case types.ProductPostgreSQL:
			// timestamptz PG specific
			testcases = append(pgDataTypeTests,
				dataTypeTestCase{
					name:       `timestamptz`,
					columnType: `TIMESTAMPTZ`,
					sqlValue:   `2016-01-25 10:10:10-05:00`,
					expectJSON: `"2016-01-25T15:10:10+00:00"`,
					indexable:  true,
				})
			readBackQ = "SELECT COALESCE(to_json(val)::VARCHAR(2048), 'null') FROM %s"
		default:
			t.Fatal("unimplemented product")
		}
	}

	for _, tc := range testcases {
		tc := tc // Capture for t.Parallel().
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := require.New(t)

			// Creating a new database for each loop is, as of v22.2 and
			// v23.1 faster than dropping the table between iterations.
			// This is relevant because the fixture contains a test
			// timeout which was getting tripped due to performance
			// changes in v23.1.
			//
			// https://github.com/cockroachdb/cockroach/issues/102259
			fixture, err := all.NewFixture(t)
			require.NoError(t, err)
			if tc.skip != nil && tc.skip(fixture) {
				t.Skip()
			}

			ctx := fixture.Context

			// Place the PK index on the data type under test, if allowable.
			var create string
			if tc.indexable {
				create = fmt.Sprintf("CREATE TABLE %%s (val %s primary key, k int)", tc.columnType)
			} else {
				create = fmt.Sprintf("CREATE TABLE %%s (k int primary key, val %s)", tc.columnType)
			}

			// Try to create the table. If the data type is unknown or
			// unimplemented, skip the test. This is justified because
			// the user couldn't have created a table in the target
			// database using this type.
			tbl, err := fixture.CreateTargetTable(ctx, create)
			if code, ok := fixture.TargetPool.ErrCode(err); ok {
				switch code {
				case "42704", // Unknown data type
					"0A000": // Feature not implemented, e.g. SERIAL[]
					t.Skipf("data type %s not supported by %s", tc.columnType,
						fixture.TargetPool.Version)
				}
			}
			r.NoError(err)
			tblName := sinktest.JumbleTable(tbl.Name())

			// Helper function to improve readability below.
			apply := fixture.Applier(ctx, tblName)

			// Use the staging connection to create a reasonable JSON blob.
			var jsonValue string
			if tc.sqlValue == "" {
				jsonValue = "null"
			} else {
				sourceType := tc.columnType
				if tc.sourceType != "" {
					sourceType = tc.sourceType
				}
				q := fmt.Sprintf("SELECT to_json($1::%s)::VARCHAR(2048)", sourceType)
				r.NoError(fixture.StagingPool.QueryRow(ctx, q, tc.sqlValue).Scan(&jsonValue))
			}
			log.Info(jsonValue)

			mut := types.Mutation{
				Data: []byte(fmt.Sprintf(`{"k":1,"val":%s}`, jsonValue)),
			}
			if tc.indexable {
				mut.Key = []byte(fmt.Sprintf(`[%s]`, jsonValue))
			} else {
				mut.Key = []byte(`[1]`)
			}
			r.NoError(apply([]types.Mutation{mut}))

			// Cross-check for delete case below.
			count, err := tbl.RowCount(ctx)
			r.NoError(err)
			r.Equal(1, count)

			expectJSON := jsonValue
			if tc.expectJSON != "" {
				expectJSON = tc.expectJSON
			}
			// Normalize the JSON representations before comparing them.
			expectJSON, err = normalizeJSON(expectJSON)
			r.NoError(err)
			var readBack string
			if tc.readBackQ != "" {
				r.NoError(fixture.TargetPool.QueryRowContext(ctx,
					fmt.Sprintf(tc.readBackQ, tbl),
				).Scan(&readBack))
				// since readBack is a string, need to quote before
				// we normalize it to JSON.
				readBack = strconv.Quote(readBack)
			} else {
				r.NoError(fixture.TargetPool.QueryRowContext(ctx,
					fmt.Sprintf(readBackQ, tbl),
				).Scan(&readBack))
				// See note in readBackQ
				if expectArrayWrapper {
					readBack = readBack[1 : len(readBack)-1]
				}
			}
			readBack, err = normalizeJSON(readBack)
			r.NoError(err)
			r.Equal(expectJSON, readBack)

			// If the type can be part of a primary key, we also want to
			// verify that we can delete instances of this type.
			if tc.indexable {
				// Turn the mutation into a deletion.
				mut.Data = nil
				r.NoError(apply([]types.Mutation{mut}))
				count, err := tbl.RowCount(ctx)
				r.NoError(err)
				r.Zero(count)
			}
		})
	}
}

// normalizeJSON re-encodes the json data. This makes it easy to perform
// semantic comparisons of JSON data without needing to consider
// non-essential differences in formatting.
func normalizeJSON(data string) (string, error) {
	var value any
	dec := json.NewDecoder(strings.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&value); err != nil {
		return "", err
	}
	buf, err := json.Marshal(value)
	return string(buf), err
}

// This tests compare-and-set behaviors.
func TestConditionals(t *testing.T) {
	t.Run("base", func(t *testing.T) { testConditions(t, false, false) })
	t.Run("cas", func(t *testing.T) { testConditions(t, true, false) })
	t.Run("deadline", func(t *testing.T) { testConditions(t, false, true) })
	t.Run("cas+deadline", func(t *testing.T) { testConditions(t, true, true) })
}

func testConditions(t *testing.T, cas, deadline bool) {
	a := assert.New(t)

	fixture, err := all.NewFixture(t)
	if !a.NoError(err) {
		return
	}

	ctx := fixture.Context

	// The payload and the table are just a key and version field.
	type Payload struct {
		PK  int       `json:"pk"`
		Ver int       `json:"ver"`
		TS  time.Time `json:"ts"`
	}

	var createStatement string
	switch fixture.TargetPool.Product {
	case types.ProductCockroachDB, types.ProductOracle, types.ProductPostgreSQL:
		createStatement = "CREATE TABLE %s (pk INT PRIMARY KEY, ver INT, ts TIMESTAMP WITH TIME ZONE)"
	case types.ProductMariaDB, types.ProductMySQL:
		createStatement = "CREATE TABLE %s (pk INT PRIMARY KEY, ver INT, ts TIMESTAMP)"
	default:
		a.FailNow("product not supported")
	}
	tbl, err := fixture.CreateTargetTable(ctx, createStatement)

	if !a.NoError(err) {
		return
	}
	// Use this when calling the APIs.
	jumbleName := sinktest.JumbleTable(tbl.Name())
	// Helper functions to improve readability below.
	apply := fixture.Applier(ctx, jumbleName)
	// Just for testing instantiation errors in factory.get.
	nilApply := func() error { return apply(nil) }

	t.Run("check_invalid_cas_name", func(t *testing.T) {
		a := assert.New(t)

		a.NoError(fixture.Configs.Set(
			jumbleName,
			applycfg.NewConfig().Patch(&applycfg.Config{
				CASColumns: []ident.Ident{ident.New("bad_column")},
			}),
		))

		err = nilApply()
		if a.Error(err) {
			a.Contains(err.Error(), "bad_column")
		}
	})

	t.Run("check_invalid_deadline_name", func(t *testing.T) {
		a := assert.New(t)

		a.NoError(fixture.Configs.Set(
			jumbleName,
			applycfg.NewConfig().Patch(&applycfg.Config{
				Deadlines: ident.MapOf[time.Duration](ident.New("bad_column"), time.Second),
			}),
		))

		err = nilApply()
		if a.Error(err) {
			a.Contains(err.Error(), "bad_column")
		}
	})

	// The PK value is a fixed value.
	const id = 42

	// Utility function to retrieve the most recently set data.
	getRow := func() (version int, ts time.Time, err error) {
		var q string
		switch fixture.TargetPool.Product {
		case types.ProductCockroachDB, types.ProductPostgreSQL:
			q = "SELECT ver, ts FROM %s WHERE pk = $1"
		case types.ProductMariaDB, types.ProductMySQL:
			// The MySQL driver returns a string for timestamp,
			// so we need to parse it.
			var res string
			err = fixture.TargetPool.QueryRowContext(ctx,
				fmt.Sprintf("SELECT ver, ts FROM %s WHERE pk = ?", tbl.Name()), id,
			).Scan(&version, &res)
			if err == nil {
				ts, err = time.Parse("2006-01-02 15:04:05", res)
			}
			return
		case types.ProductOracle:
			q = "SELECT ver, ts FROM %s WHERE pk = :p1"
		default:
			return 0, time.Time{}, errors.New("unimplemented product")
		}
		err = fixture.TargetPool.QueryRowContext(ctx,
			fmt.Sprintf(q, tbl.Name()), id,
		).Scan(&version, &ts)
		return
	}

	// Set up the apply instance, per the configuration.
	configData := applycfg.NewConfig()
	if cas {
		configData.CASColumns = []ident.Ident{ident.New("ver")}
	}
	if deadline {
		configData.Deadlines.Put(ident.New("ts"), 10*time.Minute)
	}
	a.NoError(fixture.Configs.Set(jumbleName, configData))

	now := time.Now().UTC().Round(time.Second)
	past := now.Add(-time.Hour)
	future := now.Add(time.Hour)

	proposals := []struct {
		version int
		ts      time.Time
	}{
		{0, future}, // Ensure that a zero-version can be inserted.
		{100, future},
		{5, future},
		{200, past},
		{9, past},
		{300, future},
	}
	var lastTime time.Time
	lastVersion := -1

	for _, proposed := range proposals {
		var shouldApply bool
		switch {
		case !cas && !deadline:
			shouldApply = true
		case cas && deadline:
			shouldApply = proposed.version > lastVersion && proposed.ts == future
		case cas:
			shouldApply = proposed.version > lastVersion
		case deadline:
			shouldApply = proposed.ts == future
		}

		expectedTime := lastTime
		expectedVersion := lastVersion
		if shouldApply {
			expectedTime = proposed.ts
			expectedVersion = proposed.version
		}

		p := Payload{PK: id, Ver: proposed.version, TS: proposed.ts}
		bytes, err := json.Marshal(p)
		a.NoError(err)

		// Applying a discarded mutation should never result in an error.
		a.NoError(apply([]types.Mutation{{
			Data: bytes,
			Key:  []byte(fmt.Sprintf(`[%d]`, id)),
		}}))

		ct, err := tbl.RowCount(ctx)
		a.Equal(1, ct)
		a.NoError(err)

		// Test the version that's currently in the database, make sure
		// that we haven't gone backwards.
		ver, ts, err := getRow()
		a.Equal(expectedTime.UTC(), ts.UTC())
		a.Equal(expectedVersion, ver)
		a.NoError(err)

		lastTime = expectedTime
		lastVersion = expectedVersion
	}
}

// Verifies that we can process deletes when we have a mutation body
// that aligns with the target schema, and that the replication key
// may be an arbitrary value.
func TestDeleteByBody(t *testing.T) {
	r := require.New(t)

	fixture, err := all.NewFixture(t)
	r.NoError(err)

	ctx := fixture.Context
	table, err := fixture.CreateTargetTable(ctx,
		`CREATE TABLE %s (a INT, b INT, c INT, PRIMARY KEY (a, b))`)
	r.NoError(err)

	muts := []types.Mutation{
		{
			Data: json.RawMessage(`{"a":1,"b":2,"c":3}`),
			Key:  json.RawMessage(`one`),
		},
		{
			Data: json.RawMessage(`{"a":4,"b":5,"c":6}`),
			Key:  json.RawMessage(`two`),
		},
		{
			Data: json.RawMessage(`{"a":7,"b":8,"c":9}`),
			Key:  json.RawMessage(`three`),
		},
	}

	apply := fixture.Applier(ctx, table.Name())
	r.NoError(apply(muts))

	ct, err := table.RowCount(ctx)
	r.NoError(err)
	r.Equal(len(muts), ct)

	t.Run("happy_path", func(t *testing.T) {
		r := require.New(t)
		// Now we're going to demonstrate that the replication key needs
		// only to be unique for deletions.
		for i := range muts {
			muts[i].Deletion = true
		}

		r.NoError(apply(muts))
		ct, err = table.RowCount(ctx)
		r.NoError(err)
		r.Zero(ct)
	})

	// Cook up a mutation that is missing a required PK column.
	t.Run("schema_drift", func(t *testing.T) {
		r := require.New(t)
		muts := []types.Mutation{
			{
				Data:     json.RawMessage(`{"a":1,"c":3}`),
				Deletion: true,
				Key:      json.RawMessage(`bad`),
			},
		}
		r.ErrorContains(apply(muts), "schema drift detected")
	})
}

// Verifies "constant" and substitution params, including cases where
// the PK is subject to rewriting.
func TestExpressionColumns(t *testing.T) {
	a := assert.New(t)

	fixture, err := all.NewFixture(t)
	if !a.NoError(err) {
		return
	}

	ctx := fixture.Context

	// KV payload, but with different column names.
	type Payload struct {
		PK    int    `json:"pk"`
		Val   string `json:"val"`
		Fixed string `json:"fixed"`
	}

	tbl, err := fixture.CreateTargetTable(ctx,
		"CREATE TABLE %s (pk INT PRIMARY KEY, val VARCHAR(2048), fixed VARCHAR(2048))")
	if !a.NoError(err) {
		return
	}
	// Use this jumbled-case name when calling through the API.
	jumbledName := sinktest.JumbleTable(tbl.Name())

	configData := applycfg.NewConfig()
	configData.Exprs = ident.MapOf[string](
		ident.New("pk"), "2 * $0",
		ident.New("val"), "$0 || ' world!'",
		ident.New("fixed"), "'constant'",
	)
	a.NoError(fixture.Configs.Set(jumbledName, configData))
	// Helper function to improve readability below.
	apply := fixture.Applier(ctx, jumbledName)

	p := Payload{PK: 42, Val: "Hello", Fixed: "ignored"}
	bytes, err := json.Marshal(p)
	a.NoError(err)

	a.NoError(apply([]types.Mutation{{
		Data: bytes,
		Key:  []byte(fmt.Sprintf(`[%d]`, p.PK)),
	}}))

	count, err := base.GetRowCount(ctx, fixture.TargetPool, tbl.Name())
	a.NoError(err)
	a.Equal(1, count)

	var key int
	var val string
	var fixed string
	a.NoError(fixture.TargetPool.QueryRowContext(ctx,
		fmt.Sprintf("SELECT * from %s", tbl)).Scan(&key, &val, &fixed))
	a.Equal(84, key)
	a.Equal("Hello world!", val)
	a.Equal("constant", fixed)

	// Verify that deletes with expressions work.
	a.NoError(apply([]types.Mutation{{
		Key: []byte(fmt.Sprintf(`[%d]`, p.PK)),
	}}))
	count, err = base.GetRowCount(ctx, fixture.TargetPool, tbl.Name())
	a.Equal(0, count)
	a.NoError(err)
}

// This test demonstrates how a counter table can be built that uses
// the input delta to correctly adjust a running total.
func TestMergeWiring(t *testing.T) {
	r := require.New(t)

	fixture, err := all.NewFixture(t)
	r.NoError(err)

	ctx := fixture.Context

	// KV payload, but with different column names.
	type Payload struct {
		PK  int       `json:"pk"`
		Val int       `json:"val"`
		TS  time.Time `json:"updated_at"`
	}

	var createStatement string
	switch fixture.TargetPool.Product {
	case types.ProductCockroachDB, types.ProductOracle, types.ProductPostgreSQL:
		createStatement = "CREATE TABLE %s (pk INT PRIMARY KEY, val INT, updated_at TIMESTAMP WITH TIME ZONE)"
	case types.ProductMariaDB, types.ProductMySQL:
		createStatement = "CREATE TABLE %s (pk INT PRIMARY KEY, val INT, updated_at TIMESTAMP)"
	default:
		r.FailNow("product not supported")
	}

	tbl, err := fixture.CreateTargetTable(ctx, createStatement)
	r.NoError(err)
	tblName := sinktest.JumbleTable(tbl.Name())

	var shouldDiscard, shouldDLQ, shouldTwoWay, shouldErr atomic.Bool

	configData := applycfg.NewConfig()
	configData.CASColumns = ident.Idents{ident.New("updated_at")}
	configData.Merger = merge.Func(func(ctx context.Context, con *merge.Conflict) (*merge.Resolution, error) {
		if shouldErr.Load() {
			return nil, errors.New("goodbye")
		}
		if shouldDiscard.Load() {
			return &merge.Resolution{Drop: true}, nil
		}
		if shouldDLQ.Load() {
			return &merge.Resolution{DLQ: "dlq"}, nil
		}
		valKey := ident.New("val")
		existing := coerceToInt(r, con.Target.GetZero(valKey))
		if shouldTwoWay.Load() {
			if con.Before != nil {
				return nil, errors.New("expecting nil Before")
			}
			val := coerceToInt(r, con.Target.GetZero(valKey))
			val *= 10
			con.Target.Put(valKey, val)
		} else {
			start := coerceToInt(r, con.Before.GetZero(valKey))
			end := coerceToInt(r, con.Proposed.GetZero(valKey))
			con.Target.Put(valKey, end-start+existing)
		}
		con.Target.Put(ident.New("updated_at"), time.Now())

		return &merge.Resolution{Apply: con.Target}, nil
	})
	r.NoError(fixture.Configs.Set(tblName, configData))

	// Helper function to improve readability below. This will skip the
	// test if a merge operation is not supported by the target
	// database.
	apply := func(t *testing.T, muts []types.Mutation) error {
		err := fixture.Applier(ctx, tblName)(muts)
		if err != nil && strings.HasPrefix(err.Error(), "merge operation not implemented") {
			t.Skip(err.Error())
		}
		return err
	}

	now := time.Now()
	const pk = 42
	const initial = 5

	t.Run("insert_blocking_record", func(t *testing.T) {
		r := require.New(t)
		blocking := &Payload{
			PK:  pk,
			Val: initial,
			TS:  now,
		}
		data, err := json.Marshal(blocking)
		r.NoError(err)
		r.NoError(apply(t, []types.Mutation{
			{
				Data: data,
				Key:  []byte(fmt.Sprintf("[%d]", pk)),
			},
		}))
	})

	// Generate a stale record that has a delta of 1.
	before := &Payload{
		PK:  pk,
		Val: 2,
		TS:  now.Add(-10 * time.Minute),
	}
	beforeData, err := json.Marshal(before)
	r.NoError(err)

	after := &Payload{
		PK:  pk,
		Val: 3,
		TS:  now.Add(-1 * time.Minute),
	}
	afterData, err := json.Marshal(after)
	r.NoError(err)

	stale := []types.Mutation{
		{
			Before: beforeData,
			Data:   afterData,
			Key:    []byte(fmt.Sprintf("[%d]", pk)),
		},
	}

	t.Run("discard_record", func(t *testing.T) {
		r := require.New(t)
		shouldDiscard.Store(true)
		defer shouldDiscard.Store(false)

		r.NoError(apply(t, stale))

		var val int
		r.NoError(fixture.TargetPool.QueryRowContext(ctx,
			fmt.Sprintf("SELECT val FROM %s", tbl.Name())).Scan(&val))
		r.Equal(initial, val)
	})

	t.Run("dlq_record", func(t *testing.T) {
		r := require.New(t)
		shouldDLQ.Store(true)
		defer shouldDLQ.Store(false)

		// Ensure that the DLQ table is available.
		dlqTable, err := fixture.CreateDLQTable(ctx)
		r.NoError(err)

		r.NoError(apply(t, stale))

		var val int
		r.NoError(fixture.TargetPool.QueryRowContext(ctx,
			fmt.Sprintf("SELECT val FROM %s", tbl.Name())).Scan(&val))
		r.Equal(initial, val)

		var ct int
		r.NoError(fixture.TargetPool.QueryRowContext(ctx,
			fmt.Sprintf("SELECT count(*) FROM %s", dlqTable)).Scan(&ct))
		r.Equal(1, ct)
	})

	// Insert a "stale" value representing a delta of 1.
	t.Run("merge_record", func(t *testing.T) {
		r := require.New(t)
		r.NoError(apply(t, stale))

		var val int
		r.NoError(fixture.TargetPool.QueryRowContext(ctx,
			fmt.Sprintf("SELECT val FROM %s", tbl.Name())).Scan(&val))
		r.Equal(initial+1, val)
	})

	// Check if before is unsert and if it's set to "null" token.
	stale[0].Before = nil
	t.Run("two_way_no_before", func(t *testing.T) {
		r := require.New(t)
		shouldTwoWay.Store(true)
		defer shouldTwoWay.Store(false)
		r.NoError(apply(t, stale))

		var val int
		r.NoError(fixture.TargetPool.QueryRowContext(ctx,
			fmt.Sprintf("SELECT val FROM %s", tbl.Name())).Scan(&val))
		r.Equal((initial+1)*10, val)
	})

	stale[0].Before = []byte("null")
	t.Run("two_way_null_before", func(t *testing.T) {
		r := require.New(t)
		shouldTwoWay.Store(true)
		defer shouldTwoWay.Store(false)
		r.NoError(apply(t, stale))

		var val int
		r.NoError(fixture.TargetPool.QueryRowContext(ctx,
			fmt.Sprintf("SELECT val FROM %s", tbl.Name())).Scan(&val))
		r.Equal((initial+1)*100, val)
	})

	// Finish with an update that should always be accepted.
	t.Run("expected_to_succeed", func(t *testing.T) {
		r := require.New(t)
		shouldErr.Store(true)
		defer shouldErr.Store(false)

		accepted := &Payload{
			PK:  pk,
			Val: -initial,
			TS:  now.Add(time.Hour),
		}
		data, err := json.Marshal(accepted)
		r.NoError(err)
		r.NoError(apply(t, []types.Mutation{
			{
				Data: data,
				Key:  []byte(fmt.Sprintf("[%d]", pk)),
			},
		}))

		var val int
		r.NoError(fixture.TargetPool.QueryRowContext(ctx,
			fmt.Sprintf("SELECT val FROM %s", tbl.Name())).Scan(&val))
		r.Equal(-initial, val)
	})

	r.NoError(fixture.Diagnostics.Write(ctx, io.Discard, false))
}

// This tests ignoring a primary key column, an extant db column,
// and a column which only exists in the incoming payload.
func TestIgnoredColumns(t *testing.T) {
	a := assert.New(t)

	fixture, err := all.NewFixture(t)
	if !a.NoError(err) {
		return
	}

	ctx := fixture.Context

	// KV payload, but with different column names.
	type Payload struct {
		PK0        int    `json:"pk0"`
		PKDeleted  int    `json:"pk_deleted"`
		PK1        int    `json:"pk1"`
		Val0       string `json:"val0"`
		ValIgnored string `json:"val_ignored"`
	}

	tbl, err := fixture.CreateTargetTable(ctx,
		"CREATE TABLE %s (pk0 INT, pk1 INT, val0 VARCHAR(2048), not_required VARCHAR(2048), PRIMARY KEY (pk0, pk1))")
	if !a.NoError(err) {
		return
	}
	tblName := sinktest.JumbleTable(tbl.Name())

	configData := applycfg.NewConfig()
	configData.Ignore = ident.MapOf[bool](
		ident.New("pk_deleted"), true,
		ident.New("val_ignored"), true,
		ident.New("not_required"), true,
	)
	a.NoError(fixture.Configs.Set(tblName, configData))

	// Helper function to improve readability below.
	apply := fixture.Applier(ctx, tblName)

	p := Payload{PK0: 42, PKDeleted: -1, PK1: 43, Val0: "Hello world!", ValIgnored: "Ignored"}
	bytes, err := json.Marshal(p)
	a.NoError(err)

	a.NoError(apply([]types.Mutation{{
		Data: bytes,
		Key:  []byte(fmt.Sprintf(`[%d, %d]`, p.PK0, p.PK1)),
	}}))

	// Verify deletion.
	a.NoError(apply([]types.Mutation{{
		Key: []byte(fmt.Sprintf(`[%d, %d]`, p.PK0, p.PK1)),
	}}))
}

// This tests the renaming configuration feature.
func TestRenamedColumns(t *testing.T) {
	a := assert.New(t)

	fixture, err := all.NewFixture(t)
	if !a.NoError(err) {
		return
	}

	ctx := fixture.Context

	// KV payload, but with different column names.
	type Payload struct {
		PK  int    `json:"pk_source"`
		Val string `json:"val_source"`
	}

	tbl, err := fixture.CreateTargetTable(ctx,
		"CREATE TABLE %s (pk INT PRIMARY KEY, val VARCHAR(2048))")
	if !a.NoError(err) {
		return
	}
	tblName := sinktest.JumbleTable(tbl.Name())

	configData := applycfg.NewConfig()
	configData.SourceNames = ident.MapOf[applycfg.SourceColumn](
		ident.New("pk"), ident.New("pk_source"),
		ident.New("val"), ident.New("val_source"),
	)
	a.NoError(fixture.Configs.Set(tblName, configData))
	// Helper function to improve readability below.
	apply := fixture.Applier(ctx, tblName)

	p := Payload{PK: 42, Val: "Hello world!"}
	bytes, err := json.Marshal(p)
	a.NoError(err)

	a.NoError(apply([]types.Mutation{{
		Data: bytes,
		Key:  []byte(fmt.Sprintf(`[%d]`, p.PK)),
	}}))
}

// This tests a case in which Replicator does not upsert all columns in
// the target table and where multiple updates to the same key are
// contained in the batch (which can happen in immediate mode). In this
// case, we'll see an error message that UPSERT cannot affect the same
// row multiple times.
//
// X-Ref: https://github.com/cockroachdb/cockroach/issues/44466
// X-Ref: https://github.com/cockroachdb/cockroach/pull/45372
func TestRepeatedKeysWithIgnoredColumns(t *testing.T) {
	a := assert.New(t)

	fixture, err := all.NewFixture(t)
	if !a.NoError(err) {
		return
	}

	if strings.Contains(fixture.TargetPool.Version, "PostgreSQL 11") {
		t.Skip("PostgreSQL v11 doesn't support generated columns")
	}

	ctx := fixture.Context

	type Payload struct {
		Pk0 int    `json:"pk0"`
		Val string `json:"val"`
	}

	// VIRTUAL vs. STORED
	var tblSchema string
	switch fixture.TargetPool.Product {
	case types.ProductCockroachDB, types.ProductPostgreSQL:
		tblSchema = "CREATE TABLE %s (pk0 INT PRIMARY KEY, " +
			"ignored INT GENERATED ALWAYS AS (1) STORED, " +
			"val VARCHAR(2048))"
	case types.ProductMariaDB, types.ProductMySQL:
		tblSchema = "CREATE TABLE %s (pk0 INT PRIMARY KEY, " +
			"ignored INT as (1), " +
			"val VARCHAR(2048))"
	case types.ProductOracle:
		tblSchema = "CREATE TABLE %s (pk0 INT PRIMARY KEY, " +
			"ignored INT GENERATED ALWAYS AS (1) VIRTUAL, " +
			"val VARCHAR(2048))"
	default:
		t.Fatalf("unimplemented: %s", fixture.TargetPool.Product)
	}

	tbl, err := fixture.CreateTargetTable(ctx, tblSchema)
	if !a.NoError(err) {
		return
	}
	jumbledName := sinktest.JumbleTable(tbl.Name())

	// Detect hopeful future case where UPSERT has the desired behavior.
	if fixture.TargetPool.Product == types.ProductCockroachDB {
		_, err = fixture.TargetPool.ExecContext(ctx,
			fmt.Sprintf("UPSERT INTO %s (pk0, val) VALUES ($1, $2), ($3, $4)", tbl.Name()),
			1, "1", 1, "1")
		if a.Error(err) {
			a.Contains(err.Error(), "cannot affect row a second time")
		} else {
			a.FailNow("the workaround is no longer necessary for this version of CRDB")
		}
	}

	// Helper function to improve readability below.
	apply := fixture.Applier(ctx, jumbledName)

	p1 := Payload{Pk0: 10, Val: "First"}
	bytes1, err := json.Marshal(p1)
	a.NoError(err)

	p2 := Payload{Pk0: 10, Val: "Repeated"}
	bytes2, err := json.Marshal(p2)
	a.NoError(err)

	muts := []types.Mutation{
		{Data: bytes1, Key: []byte(fmt.Sprintf(`[%d]`, p1.Pk0)), Time: hlc.New(1, 1)},
		{Data: bytes2, Key: []byte(fmt.Sprintf(`[%d]`, p2.Pk0)), Time: hlc.New(1, 2)},
	}

	// Verify insertion.
	a.NoError(apply(muts))

	count, err := base.GetRowCount(ctx, fixture.TargetPool, tbl.Name())
	if a.NoError(err) && a.Equal(1, count) {
		var q string
		switch fixture.TargetPool.Product {
		case types.ProductCockroachDB, types.ProductPostgreSQL:
			q = "SELECT val FROM %s WHERE pk0 = $1"
		case types.ProductMariaDB, types.ProductMySQL:
			q = "SELECT val FROM %s WHERE pk0 = ?"
		case types.ProductOracle:
			q = "SELECT val FROM %s WHERE pk0 = :pk"
		default:
			a.Fail("unimplemented product")
		}
		row := fixture.TargetPool.QueryRowContext(ctx,
			fmt.Sprintf(q, tbl.Name()), 10)
		var val string
		a.NoError(row.Scan(&val))
		a.Equal("Repeated", val)
	}
}

// Verify that sparse payloads will load pre-existing values from the
// target table.
func TestSparsePayload(t *testing.T) {
	const numRows = 128
	r := require.New(t)

	fixture, err := all.NewFixture(t)
	r.NoError(err)
	ctx := fixture.Context

	// KV payload, but with different column names.
	type Payload struct {
		PK   int    `json:"pk"`
		Val0 string `json:"val0,omitempty"`
		Val1 string `json:"val1,omitempty"`
		Val2 string `json:"val2,omitempty"`
		Val3 string `json:"val3,omitempty"`
	}
	newPayload := func(pk int) *Payload {
		return &Payload{
			PK:   pk,
			Val0: "val0_initial",
			Val1: "val1_initial",
			Val2: "val2_initial",
			Val3: "val3_initial",
		}
	}

	tbl, err := fixture.CreateTargetTable(ctx, `
CREATE TABLE %s (
  pk INT PRIMARY KEY,
  val0 VARCHAR(32),
  val1 VARCHAR(32) DEFAULT 'should_not_see_this',
  val2 VARCHAR(32) NOT NULL,
  val3 VARCHAR(32) DEFAULT 'should_not_see_this' NOT NULL
)`)
	r.NoError(err)
	app := fixture.Applier(ctx, tbl.Name())

	// Insert initial values.
	muts := make([]types.Mutation, numRows)
	for i := range muts {
		p := newPayload(i)
		data, err := json.Marshal(p)
		r.NoError(err)
		muts[i] = types.Mutation{
			Data: data,
			Key:  []byte(fmt.Sprintf(`[%d]`, i)),
		}
	}
	r.NoError(app(muts))

	// Generate sparse values and the expected final states.
	expected := make([]*Payload, numRows)
	patches := make([]types.Mutation, numRows)
	for i := range patches {
		next := newPayload(i)
		patchP := &Payload{PK: i}
		if i&(1<<0) != 0 {
			next.Val0 = "val0_updated"
			patchP.Val0 = next.Val0
		}
		if i&(1<<1) != 0 {
			next.Val1 = "val1_updated"
			patchP.Val1 = next.Val1
		}
		if i&(1<<2) != 0 {
			next.Val2 = "val2_updated"
			patchP.Val2 = next.Val2
		}
		expected[i] = next
		patchData, err := json.Marshal(patchP)
		r.NoError(err)
		patches[i] = types.Mutation{
			Data: patchData,
			Key:  []byte(fmt.Sprintf(`[%d]`, i)),
		}
	}
	// Generate supplemental, sparse mutations for a subset of the keys.
	// This ensures that two mutations for the same key in the same
	// target transaction will have their data elements folded together.
	for i := range patches {
		if i%7 == 0 {
			expected[i].Val3 = "val3_updated"

			patchP := &Payload{PK: i}
			patchP.Val3 = expected[i].Val3

			patchData, err := json.Marshal(patchP)
			r.NoError(err)
			patches = append(patches, types.Mutation{
				Data: patchData,
				Key:  []byte(fmt.Sprintf(`[%d]`, i)),
			})
		}
		if i%11 == 0 {
			expected[i] = &Payload{PK: i}

			patches = append(patches, types.Mutation{
				Deletion: true,
				Key:      []byte(fmt.Sprintf(`[%d]`, i)),
			})
		}
	}

	r.NoError(app(patches))

	found := make([]*Payload, numRows)
	for i := range found {
		next := &Payload{PK: i}
		err := fixture.TargetPool.QueryRow(fmt.Sprintf(
			"SELECT val0, val1, val2, val3 FROM %s WHERE pk=%d", tbl.Name(), i,
		)).Scan(&next.Val0, &next.Val1, &next.Val2, &next.Val3)
		// Handle deletion case.
		if !errors.Is(err, sql.ErrNoRows) {
			r.NoError(err)
		}
		found[i] = next
	}

	r.Equal(expected, found)
}

// Verify that user-defined enums with mixed-case identifiers work.
func TestUDTEnum(t *testing.T) {

	fixture, err := all.NewFixture(t)
	require.NoError(t, err)

	if fixture.TargetPool.Product != types.ProductCockroachDB {
		t.Skip("test not relevant to product")
	}

	ctx := fixture.Context

	type Payload struct {
		PK  int `json:"pk"`
		Val any `json:"val,omitempty"`
	}

	_, err = fixture.TargetPool.ExecContext(ctx, fmt.Sprintf(
		`CREATE TYPE %s."MyEnum" AS ENUM ('foo', 'bar')`,
		fixture.TargetSchema.Schema()))
	require.NoError(t, err)

	t.Run("scalar", func(t *testing.T) {
		r := require.New(t)
		tbl, err := fixture.CreateTargetTable(ctx,
			fmt.Sprintf(`CREATE TABLE %%s (pk INT PRIMARY KEY, val %s."MyEnum")`,
				fixture.TargetSchema.Schema()))
		r.NoError(err)
		tblName := sinktest.JumbleTable(tbl.Name())

		p := Payload{PK: 42, Val: "bar"}
		bytes, err := json.Marshal(p)
		r.NoError(err)

		apply := fixture.Applier(ctx, tblName)
		r.NoError(apply([]types.Mutation{{
			Data: bytes,
			Key:  []byte(fmt.Sprintf(`[%d]`, p.PK)),
		}}))
	})

	t.Run("array", func(t *testing.T) {
		r := require.New(t)
		tbl, err := fixture.CreateTargetTable(ctx,
			fmt.Sprintf(`CREATE TABLE %%s (pk INT PRIMARY KEY, val %s."MyEnum"[])`,
				fixture.TargetSchema.Schema()))
		r.NoError(err)
		tblName := sinktest.JumbleTable(tbl.Name())

		p := Payload{PK: 42, Val: []string{"foo", "bar"}}
		bytes, err := json.Marshal(p)
		r.NoError(err)

		apply := fixture.Applier(ctx, tblName)
		r.NoError(apply([]types.Mutation{{
			Data: bytes,
			Key:  []byte(fmt.Sprintf(`[%d]`, p.PK)),
		}}))
	})
}

// Ensure that if columns with generation expressions are present, we
// don't try to write to them and that we correctly ignore those columns
// in incoming key and data payloads.
func TestVirtualColumns(t *testing.T) {
	a := assert.New(t)

	fixture, err := all.NewFixture(t)
	if !a.NoError(err) {
		return
	}

	switch fixture.TargetPool.Product {
	case types.ProductMariaDB, types.ProductMySQL:
		// In MySQL/MariaDB, defining a virtual generated column as primary key
		// is not supported. Handle them separately.
		testVirtualColumnsMySQL(t, fixture)
		return
	default:
		// Keep going for the rest of the products.
	}
	if strings.Contains(fixture.TargetPool.Version, "PostgreSQL 11") {
		t.Skip("PostgreSQL v11 doesn't support generated columns")
	}

	ctx := fixture.Context

	type Payload struct {
		A  int `json:"a"`
		B  int `json:"b"`
		C  int `json:"c"`
		CK int `json:"ck"`
		X  int `json:"x,omitempty"`
	}

	tblDef := "CREATE TABLE %s (" +
		"a INT, " +
		"ck INT GENERATED ALWAYS AS (a + b) VIRTUAL, " +
		"b INT, " +
		"c INT GENERATED ALWAYS AS (a + b + 1) VIRTUAL, " +
		"PRIMARY KEY (a,ck,b))"
	// CRDB v21 didn't yet implement virtual columns in PKs.
	// PostgreSQL has no virtual column support at all.
	if fixture.TargetPool.Product == types.ProductPostgreSQL ||
		strings.Contains(fixture.TargetPool.Version, "v21.") {
		tblDef = strings.ReplaceAll(tblDef, "VIRTUAL", "STORED")
	}

	tbl, err := fixture.CreateTargetTable(ctx, tblDef)
	if !a.NoError(err) {
		return
	}

	t.Run("computed-is-ignored", func(t *testing.T) {
		a := assert.New(t)
		p := Payload{A: 1, B: 2, C: 3, CK: 3}
		bytes, err := json.Marshal(p)
		a.NoError(err)
		tblName := sinktest.JumbleTable(tbl.Name())
		muts := []types.Mutation{{
			Data: bytes,
			Key:  []byte(fmt.Sprintf(`[%d, %d, %d]`, p.A, p.CK, p.B)),
		}}

		apply := fixture.Applier(ctx, tblName)
		a.NoError(apply(muts))
	})

	t.Run("unknown-still-breaks", func(t *testing.T) {
		a := assert.New(t)
		p := Payload{A: 1, B: 2, C: 3, X: -1}
		bytes, err := json.Marshal(p)
		a.NoError(err)
		tblName := sinktest.JumbleTable(tbl.Name())
		muts := []types.Mutation{{
			Data: bytes,
			Key:  []byte(fmt.Sprintf(`[%d, %d, %d]`, p.A, p.CK, p.B)),
		}}

		apply := fixture.Applier(ctx, tblName)
		err = apply(muts)
		if a.Error(err) {
			a.Contains(err.Error(), "unexpected columns")
		}
	})

	t.Run("deletes", func(t *testing.T) {
		a := assert.New(t)
		p := Payload{A: 1, B: 2, C: 3, CK: 3}
		a.NoError(err)
		tblName := sinktest.JumbleTable(tbl.Name())
		muts := []types.Mutation{{
			Key: []byte(fmt.Sprintf(`[%d, %d, %d]`, p.A, p.CK, p.B)),
		}}

		apply := fixture.Applier(ctx, tblName)
		a.NoError(apply(muts))
	})
}

// In MySQL and MariaDB, defining a virtual generated column as primary key is not supported
// Ensure that if columns with generation expressions are present, we
// don't try to write to them and that we correctly ignore those columns
// in data payloads.
func testVirtualColumnsMySQL(t *testing.T, fixture *all.Fixture) {
	a := assert.New(t)

	ctx := fixture.Context

	type Payload struct {
		A int `json:"a"`
		B int `json:"b"`
		C int `json:"c"`
		D int `json:"d"`
		X int `json:"x,omitempty"`
	}

	tblDef := "CREATE TABLE %s (" +
		"a INT, " +
		"b INT, " +
		"c INT AS (a + b + 1) VIRTUAL, " +
		"d INT AS (a + b) VIRTUAL, " +
		"PRIMARY KEY (a,b))"
	tbl, err := fixture.CreateTargetTable(ctx, tblDef)
	if !a.NoError(err) {
		return
	}

	t.Run("computed-is-ignored", func(t *testing.T) {
		a := assert.New(t)
		p := Payload{A: 1, B: 2, C: 3, D: 3}
		bytes, err := json.Marshal(p)
		a.NoError(err)
		tblName := sinktest.JumbleTable(tbl.Name())
		muts := []types.Mutation{{
			Data: bytes,
			Key:  []byte(fmt.Sprintf(`[%d, %d, %d]`, p.A, p.D, p.B)),
		}}

		apply := fixture.Applier(ctx, tblName)
		a.NoError(apply(muts))
	})

	t.Run("unknown-still-breaks", func(t *testing.T) {
		a := assert.New(t)
		p := Payload{A: 1, B: 2, C: 3, X: -1}
		bytes, err := json.Marshal(p)
		a.NoError(err)
		tblName := sinktest.JumbleTable(tbl.Name())
		muts := []types.Mutation{{
			Data: bytes,
			Key:  []byte(fmt.Sprintf(`[%d, %d, %d]`, p.A, p.D, p.B)),
		}}

		apply := fixture.Applier(ctx, tblName)
		err = apply(muts)
		if a.Error(err) {
			a.Contains(err.Error(), "unexpected columns")
		}
	})

	t.Run("deletes", func(t *testing.T) {
		a := assert.New(t)
		p := Payload{A: 1, B: 2, C: 3, D: 3}
		a.NoError(err)
		tblName := sinktest.JumbleTable(tbl.Name())
		muts := []types.Mutation{{
			Key: []byte(fmt.Sprintf(`[%d, %d]`, p.A, p.B)),
		}}

		apply := fixture.Applier(ctx, tblName)
		a.NoError(apply(muts))
	})
}

type benchConfig struct {
	name          string
	batchSize     int
	cas, deadline bool
}

func BenchmarkApply(b *testing.B) {
	templates := []benchConfig{
		{name: "base"},
		{name: "cas", cas: true},
		{name: "deadline", deadline: true},
		{name: "cas+deadline", cas: true, deadline: true},
	}
	sizes := []int{1, 10, 100, 1_000, 10_000}

	tcs := make([]benchConfig, 0, len(templates)*len(sizes))
	for _, size := range sizes {
		for _, tmpl := range templates {
			// struct, not pointer type.
			tmpl.batchSize = size
			tcs = append(tcs, tmpl)
		}
	}

	for _, tc := range tcs {
		b.Run(tc.name, func(b *testing.B) {
			benchConditions(b, tc)
		})
	}
}

func benchConditions(b *testing.B, cfg benchConfig) {
	a := assert.New(b)

	fixture, err := all.NewFixture(b)
	if !a.NoError(err) {
		return
	}

	ctx := fixture.Context

	tbl, err := fixture.CreateTargetTable(ctx,
		"CREATE TABLE %s (pk UUID PRIMARY KEY, ver INT, ts TIMESTAMP)")
	if !a.NoError(err) {
		return
	}
	tblName := sinktest.JumbleTable(tbl.Name())

	// Set up the apply instance, per the configuration.
	configData := applycfg.NewConfig()
	if cfg.cas {
		configData.CASColumns = []ident.Ident{ident.New("ver")}
	}
	if cfg.deadline {
		configData.Deadlines.Put(ident.New("ts"), time.Hour)
	}
	a.NoError(fixture.Configs.Set(tblName, configData))

	// Helper function to improve readability below.
	apply := fixture.Applier(ctx, tblName)

	// Create a source of Mutatation data.
	muts := mutations.Generator(ctx, 100000, 0)

	var loopTotal atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		batch := make([]types.Mutation, cfg.batchSize)
		loops := int64(0)

		for pb.Next() {
			for i := range batch {
				mut := <-muts
				batch[i] = mut
				loopTotal.Add(int64(len(mut.Data)))
			}

			// Applying a discarded mutation should never result in an error.
			if !a.NoError(apply(batch)) {
				return
			}
			loops++
		}
	})

	// Use bytes as a throughput measure.
	b.SetBytes(loopTotal.Load())
}

func coerceToInt(r *require.Assertions, x any) int {
	switch t := x.(type) {
	case int:
		return t
	case int32:
		return int(t)
	case int64:
		return int(t)
	case json.Number:
		i, err := t.Int64()
		r.NoError(err)
		return int(i)
	default:
		r.Failf("unsupported type", "%T", t)
		return 0
	}
}
