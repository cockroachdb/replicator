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

package oracleparser

import (
	"fmt"
	"testing"

	"github.com/antlr4-go/antlr/v4"
	"github.com/cockroachdb/datadriven"
	orclantl "github.com/cockroachdb/replicator/internal/util/oracleparser/thirdparty"
	"github.com/stretchr/testify/require"
)

func TestDataDriven(t *testing.T) {

	const testdataPath = `./testdata`
	datadriven.Walk(t, testdataPath, func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, td *datadriven.TestData) string {
			var err error
			var strRes string
			switch td.Cmd {
			case "parse":
				var expectError bool
				for _, arg := range td.CmdArgs {
					switch arg.Key {
					case "error":
						expectError = true
					default:
						t.Errorf("unknown cmd arg %s", arg.Key)
					}
				}
				lexer := orclantl.NewPlSqlLexer(antlr.NewInputStream(td.Input))
				stream := antlr.NewCommonTokenStream(lexer, 0)
				p := orclantl.NewPlSqlParser(stream)
				tree := p.Sql_script()

				valKV := make(map[string]interface{})
				whereKV := make(map[string]interface{})

				l := &MockListener{
					SetAndWhere: SetAndWhereKVStructs{
						SetKV:   valKV,
						WhereKV: whereKV,
					},
				}
				antlr.ParseTreeWalkerDefault.Walk(l, tree)
				if l.Err != nil {
					return l.Err.Error()
				}
				strRes, err = l.SetAndWhere.SetKV.String()
				if !expectError {
					require.NoError(t, err)
				} else {
					return err.Error()
				}

				if len(l.SetAndWhere.WhereKV) > 0 {
					whereRes, err := l.SetAndWhere.WhereKV.String()
					if !expectError {
						require.NoError(t, err)
					} else {
						return err.Error()
					}
					strRes = fmt.Sprintf("%s\nWHERE\n%s", strRes, whereRes)
				}

				return strRes
			default:
				t.Fatalf("uknown cmd")
			}
			return ""
		})
	})
}
