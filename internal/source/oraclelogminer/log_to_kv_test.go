// Copyright 2024 The Cockroach Authors
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

package oraclelogminer

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/replicator/internal/util/oracleparser"
	"github.com/stretchr/testify/require"
)

type logToKVTestCase struct {
	input          string
	expectedKV     oracleparser.KVStruct
	expectedErrStr string
}

func TestLogToKV(t *testing.T) {
	for i, tc := range []logToKVTestCase{
		{
			input:      `insert into "C##MYADMIN"."EMPLOYEE"("ID","NAME_TARGET","SALARY") values ('1','hello','123');`,
			expectedKV: oracleparser.KVStruct{"ID": "1", "NAME_TARGET": "hello", "SALARY": "123"},
		},
		{
			input:      `update "C##MYADMIN"."EMPLOYEE" set "SALARY" = '222' where "SALARY" = '123' and ROWID = 'AAASLzAAHAAAAFeAAA';`,
			expectedKV: oracleparser.KVStruct{"SALARY": "222"},
		},
		{
			input: `insert into "C##MYADMIN"."USERTABLE"(
                                     "YCSB_KEY","FIELD0","FIELD1","FIELD2","FIELD3",
                                     "FIELD4","FIELD5","FIELD6","FIELD7","FIELD8",
                                     "FIELD9","SOURCE_TIMESTAMP") 
					values (
					        'user8285893862370130496',
					        '(Fe?2l$N)-9&"<<-_\x7f8$4,Es7 0=D+,A+(($&?z*7j',
					        '2M'')364^k3D%<[/-V=4;6=L)!H58C''$Ss:3j''%.-U5',
					        '$60%:|&^}/*&<I+8];''+~3E-/_e"2n''Mm/.r&.($Qs',
					        NULL,NULL,NULL,NULL,NULL,NULL,NULL,
					        TO_TIMESTAMP('24-09-10 12:53:38,543044000'));`,

			expectedKV: oracleparser.KVStruct{"FIELD0": "(Fe?2l$N)-9&\"<<-_\\x7f8$4,Es7 0=D+,A+(($&?z*7j", "FIELD1": "2M'')364^k3D%<[/-V=4;6=L)!H58C''$Ss:3j''%.-U5", "FIELD2": "$60%:|&^}/*&<I+8];''+~3E-/_e\"2n''Mm/.r&.($Qs", "FIELD3": nil, "FIELD4": nil, "FIELD5": nil, "FIELD6": nil, "FIELD7": nil, "FIELD8": nil, "FIELD9": nil, "SOURCE_TIMESTAMP": "24-09-10 12:53:38,543044000", "YCSB_KEY": "user8285893862370130496"},
		},
		{
			input: `update "C##MYADMIN"."USERTABLE" set 
                                    "FIELD0" = '4X)-^o8Q31Yi>V)12p4Dc"T?$Bu6_k-D916h?$"4\+',
                                    "SOURCE_TIMESTAMP" = TO_TIMESTAMP('24-09-10 12:53:38,532918000') 
                                where "FIELD0" = '?Z;9?,>Be#Yg<6b&U#%:0=)b582$Lu)\o7/v?[m/]m' 
                                  and "SOURCE_TIMESTAMP" = TO_TIMESTAMP('24-09-10 12:53:38,487742000') 
                                  and ROWID = 'AAASMEAAHAAAAFdAAZ';`,
			expectedKV: oracleparser.KVStruct{"FIELD0": "4X)-^o8Q31Yi>V)12p4Dc\"T?$Bu6_k-D916h?$\"4\\+", "SOURCE_TIMESTAMP": "24-09-10 12:53:38,532918000"},
		},
		{
			input:      `delete from "C##MYADMIN"."USERTABLE" where "YCSB_KEY" = 'user8285893862370130496' and "FIELD0" = '(Fe?2l$N)-9&"<<-_\x7f8$4,Es7 0=D+,A+(($&?z*7j' and "FIELD1" = '2M'')364^k3D%<[/-V=4;6=L)!H58C''$Ss:3j''%.-U5' and "FIELD2" = '$60%:|&^}/*&<I+8];''+~3E-/_e"2n''Mm/.r&.($Qs' and "FIELD3" IS NULL and "FIELD4" IS NULL and "FIELD5" IS NULL and "FIELD6" IS NULL and "FIELD7" IS NULL and "FIELD8" IS NULL and "FIELD9" IS NULL and "SOURCE_TIMESTAMP" = TO_TIMESTAMP('24-09-10 12:53:38,543044000') and ROWID = 'AAASMEAAHAAAAFfAAE';`,
			expectedKV: make(oracleparser.KVStruct),
		},
		{
			input:      `INSERT INTO hello (TSCOL) VALUES (TO_TIMESTAMP('24-09-10 12:53:38,543044000'));`,
			expectedKV: oracleparser.KVStruct{"TSCOL": "24-09-10 12:53:38,543044000"},
		},
	} {
		t.Run(fmt.Sprintf("subtest-%d", i), func(t *testing.T) {
			actual, err := LogToKV(tc.input)
			if tc.expectedErrStr != "" {
				require.ErrorContains(t, err, tc.expectedErrStr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedKV, actual)
			}
		})
	}
}
