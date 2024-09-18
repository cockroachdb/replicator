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
	"fmt"
	"regexp"
	"strings"

	orclantl "github.com/cockroachdb/replicator/internal/util/oracleparser/thirdparty"
	"github.com/pingcap/errors"
)

// MockListener is the actual listener to expose to the parser.
type MockListener struct {
	orclantl.BasePlSqlParserListener
	KV  KVStruct
	Err error
}

const (
	EmptyClobStr   = `EMPTY_CLOB()`
	NullStr        = `NULL`
	ToTimestampFmt = `TO_TIMESTAMP\(\'([0-9A-Z\. :,\-]+)\'\)`
)

const ErrHeader = `ERROR`

type EmptyClob struct{}

func (c *EmptyClob) MarshalJSON() ([]byte, error) {
	return []byte(`"ORACLE_EMPTY_CLOB()"`), nil
}

// Column Name > Value
type KVStruct map[string]interface{}

func (kv KVStruct) String() (string, error) {
	byteRes, err := json.MarshalIndent(kv, "", "  ")
	if err != nil {
		return "", err
	}
	return string(byteRes), nil
}

// EnterUpdate_statement implements antlr.PlSqlParserListener.
func (s *MockListener) EnterUpdate_statement(ctx *orclantl.Update_statementContext) {
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
		case EmptyClobStr:
			// The first entry of the children is Column Name Context.
			treeNode := children[i+1]
			if _, ok := treeNode.(*orclantl.ExpressionContext); ok {
				s.KV[colName] = &EmptyClob{}
			}
		case NullStr:
			s.KV[colName] = nil
		default:
			toTimestampRe := regexp.MustCompile(ToTimestampFmt)
			if toTimestampRe.MatchString(valStr) {
				matches := toTimestampRe.FindStringSubmatch(valStr)
				if len(matches) < 2 {
					panic(fmt.Sprintf("matching TO_TIMESTAMP pattern but no matched group"))
				}
				valStr = matches[1]
			}
			s.KV[colName] = valStr
		}
	}
}

// EnterInsert_statement implements antlr.PlSqlParserListener.
func (s *MockListener) EnterInsert_statement(ctx *orclantl.Insert_statementContext) {
	singTblInsert := ctx.Single_table_insert()
	insertInto := singTblInsert.Insert_into_clause()
	parenColList := insertInto.Paren_column_list()
	if parenColList == nil {
		s.Err = errors.Wrap(errors.New(fmt.Sprintf("column list is not specified")), ErrHeader)
		return
	}
	colList := parenColList.Column_list()
	valChildren := singTblInsert.Values_clause().GetChildren()
	ValList := singTblInsert.Values_clause().Expressions().AllExpression()

	colNames := colList.AllColumn_name()
	for i, col := range colNames {
		colName := col.GetText()
		valStr := ValList[i].GetText()
		if strings.HasPrefix(colName, `"`) && strings.HasSuffix(colName, `"`) {
			colName = strings.TrimPrefix(strings.TrimSuffix(colName, `"`), `"`)
		}
		if strings.HasPrefix(valStr, `'`) && strings.HasSuffix(valStr, `'`) {
			valStr = strings.TrimPrefix(strings.TrimSuffix(valStr, `'`), `'`)
		}
		switch valStr {
		case EmptyClobStr:
			treeNode := valChildren[i]
			if _, ok := treeNode.(*orclantl.ExpressionsContext); ok {
				s.KV[colName] = &EmptyClob{}
			}
		case NullStr:
			s.KV[colName] = nil
		default:
			toTimestampRe := regexp.MustCompile(ToTimestampFmt)
			if toTimestampRe.MatchString(valStr) {
				matches := toTimestampRe.FindStringSubmatch(valStr)
				if len(matches) < 2 {
					panic(fmt.Sprintf("matching TO_TIMESTAMP pattern but no matched group"))
				}
				valStr = matches[1]
			}
			s.KV[colName] = valStr
		}
	}
}
