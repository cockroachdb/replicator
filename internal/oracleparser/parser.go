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

import "github.com/antlr4-go/antlr/v4"

// PlSqlLexerBase implementation.
type PlSqlLexerBase struct {
	*antlr.BaseLexer
}

// IsNewlineAtPos is a required definition for PlSqlLexer.
func (l *PlSqlLexer) IsNewlineAtPos(pos int) bool {
	stream := l.GetInputStream()
	la := stream.LA(pos)
	return la == -1 || la == '\n'
}

// PlSqlParserBase implementation.
type PlSqlParserBase struct {
	*antlr.BaseParser
}

// isVersion12 is a required definition for PlSqlParserBase.
func (p *PlSqlParserBase) isVersion12() bool {
	return true
}

// isVersion10 is a required definition for PlSqlParserBase.
func (p *PlSqlParserBase) isVersion10() bool {
	return false
}
