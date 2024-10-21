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
	"github.com/antlr4-go/antlr/v4"
	"github.com/cockroachdb/replicator/internal/util/oracleparser"
	orclantl "github.com/cockroachdb/replicator/internal/util/oracleparser/thirdparty"
)

// LogToKV parse a sql stmt log to a SetKV struct, where the key is the column to be rewritten, the value
// is the value to override / insert. The true extraction logic can be found in the functions related
// to oracleparser.MockListener.
// Examples can be found in TestLogToKV().
func LogToKV(log string) (oracleparser.SetAndWhereKVStructs, error) {
	lexer := orclantl.NewPlSqlLexer(antlr.NewInputStream(log))
	stream := antlr.NewCommonTokenStream(lexer, 0)
	p := orclantl.NewPlSqlParser(stream)
	tree := p.Sql_script()

	valKV := make(map[string]interface{})
	whereKV := make(map[string]interface{})

	l := &oracleparser.MockListener{
		SetAndWhere: oracleparser.SetAndWhereKVStructs{
			SetKV:   valKV,
			WhereKV: whereKV,
		},
	}

	antlr.ParseTreeWalkerDefault.Walk(l, tree)
	if l.Err != nil {
		return l.SetAndWhere, l.Err
	}

	return l.SetAndWhere, nil
}
