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
	"encoding/json"
	"strings"
	"testing"

	"github.com/antlr4-go/antlr/v4"
	"github.com/cockroachdb/datadriven"
	orclantl "github.com/cockroachdb/replicator/internal/oracleparser/thirdparty"
	"github.com/stretchr/testify/require"
)

// mockListener is the actual listener to expose to the parser.
type mockListener struct {
	orclantl.BasePlSqlParserListener
	kv kvStruct
}

const (
	emptyClobStr = `EMPTY_CLOB()`
	nullStr      = `NULL`
)

type emptyClob struct{}

func (c *emptyClob) MarshalJSON() ([]byte, error) {
	return []byte(`"ORACLE_EMPTY_CLOB()"`), nil
}

// Column Name > Value
type kvStruct map[string]interface{}

func (kv kvStruct) String() (string, error) {
	byteRes, err := json.MarshalIndent(kv, "", "  ")
	if err != nil {
		return "", err
	}
	return string(byteRes), nil
}

func (s *mockListener) EnterUpdate_statement(ctx *orclantl.Update_statementContext) {
	set := ctx.Update_set_clause()
	allColSets := set.AllColumn_based_update_set_clause()
	for i, colSet := range allColSets {
		colName := colSet.Column_name().GetText()
		valStr := colSet.Expression().GetText()
		children := colSet.GetChildren()
		if strings.HasPrefix(colName, `"`) && strings.HasSuffix(colName, `"`) {
			colName = strings.TrimPrefix(strings.TrimSuffix(colName, `"`), `"`)
		}
		if strings.HasPrefix(valStr, `'`) && strings.HasSuffix(valStr, `'`) {
			valStr = strings.TrimPrefix(strings.TrimSuffix(valStr, `'`), `'`)
		}

		switch valStr {
		case emptyClobStr:
			// The first entry of the children is Column Name Context.
			treeNode := children[i+1]
			if _, ok := treeNode.(*orclantl.ExpressionContext); ok {
				s.kv[colName] = &emptyClob{}
			}
		case nullStr:
			s.kv[colName] = nil
		default:
			s.kv[colName] = valStr
		}
	}
}

func (s *mockListener) EnterInsert_statement(ctx *orclantl.Insert_statementContext) {
	singTblInsert := ctx.Single_table_insert()
	insertInto := singTblInsert.Insert_into_clause()
	colList := insertInto.Paren_column_list().Column_list()
	valChildren := singTblInsert.Values_clause().GetChildren()
	ValList := singTblInsert.Values_clause().Expressions().AllExpression()

	colNames := colList.AllColumn_name()
	for i, col := range colNames {
		colName := col.GetText()
		valStr := ValList[i].GetText()
		if strings.HasPrefix(colName, "\"") && strings.HasSuffix(colName, "\"") {
			colName = strings.TrimPrefix(strings.TrimSuffix(colName, "\""), "\"")
		}
		if strings.HasPrefix(valStr, "'") && strings.HasSuffix(valStr, "'") {
			valStr = strings.TrimPrefix(strings.TrimSuffix(valStr, "'"), "'")
		}
		switch valStr {
		case emptyClobStr:
			treeNode := valChildren[i]
			if _, ok := treeNode.(*orclantl.ExpressionsContext); ok {
				s.kv[colName] = &emptyClob{}
			}
		case nullStr:
			s.kv[colName] = nil
		default:
			s.kv[colName] = valStr
		}
	}
}

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

				l := &mockListener{
					kv: make(map[string]interface{}),
				}
				antlr.ParseTreeWalkerDefault.Walk(l, tree)
				strRes, err = l.kv.String()
				if !expectError {
					require.NoError(t, err)
				} else {
					return err.Error()
				}

				return strRes
			default:
				t.Fatalf("uknown cmd")
			}
			return ""
		})
	})
}
