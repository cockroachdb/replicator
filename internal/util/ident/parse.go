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

package ident

import (
	"strings"
	"unicode/utf8"

	"github.com/pkg/errors"
)

const (
	quote     rune = '"'
	separator rune = '.'
)

// ParseIdent extracts the first, possibly-quoted, Ident from the given
// string. It returns the parsed Ident and the remainder of the input
// string. An error is returned if there is an unmatched double-quote
// character.
func ParseIdent(s string) (Ident, string, error) {
	if s == "" {
		return Ident{}, "", errors.New("cannot parse empty string")
	}
	switch rune(s[0]) {
	case separator:
		return Ident{}, "", errors.New("cannot start with separator")
	case quote:
		return parseQuotedIdent(s)
	default:
		return parseUnquotedIdent(s)
	}
}

// parseQuotedIdent parses an Ident that has been wrapped in quotes. It
// scans ahead, looking for the terminating quote.
func parseQuotedIdent(s string) (Ident, string, error) {
	// Look for the next unescaped quote mark.
	lastQuoteIdx := -1
	var prev rune
	for idx := 1; idx < len(s); {
		next, size := utf8.DecodeRuneInString(s[idx:])
		if next == utf8.RuneError {
			return Ident{}, "", errors.New("malformed UTF8 input")
		}

		if next == quote {
			if prev == quote {
				// Reading through an escaped quote character.
				lastQuoteIdx = -1
				prev = 0
				idx += size
				continue
			}
			lastQuoteIdx = idx
		} else if prev == quote {
			// Read past the closing quote, so break the loop.
			break
		}
		prev = next
		idx += size
	}
	if lastQuoteIdx == -1 {
		return Ident{}, "", errors.New("did not find trailing quote")
	}

	unescaped := strings.ReplaceAll(s[1:lastQuoteIdx], `""`, `"`)
	return New(unescaped), s[lastQuoteIdx+1:], nil
}

// parseUnquotedIdent looks ahead for the next separator.
func parseUnquotedIdent(s string) (Ident, string, error) {
	var idx int
loop:
	for idx < len(s) {
		next, size := utf8.DecodeRuneInString(s[idx:])
		switch next {
		case separator:
			break loop
		case utf8.RuneError:
			return Ident{}, "", errors.New("malformed UTF8 input")
		default:
			idx += size
		}
	}

	return New(s[:idx]), s[idx:], nil
}

// Qualification is a return value from ParseTable, indicating how many
// name parts were present in the initial input.
type Qualification int

//go:generate go run golang.org/x/tools/cmd/stringer -type=Qualification

// Various levels of table-identifier qualification.
const (
	TableOnly Qualification = iota + 1
	TableAndDatabase
	FullyQualified
)

// ParseTable parses a table name, relative to a base schema.
// The string must have one to three parts, separated by a dot.
func ParseTable(s string, relativeTo Schema) (Table, Qualification, error) {
	var err error
	parts := make([]Ident, 0, 3)
	for s != "" {
		var part Ident

		part, s, err = ParseIdent(s)
		if err != nil {
			return Table{}, 0, err
		}
		parts = append(parts, part)

		// Skip next dot, if any.
		if s == "" {
			break
		}
		if rune(s[0]) != separator {
			return Table{}, 0, errors.New("expecting separator")
		}
		s = s[1:]
	}

	switch len(parts) {
	case 0:
		return Table{}, 0, errors.New("empty table name")

	case 1:
		return NewTable(
				relativeTo.Database(),
				relativeTo.Schema(),
				parts[0]),
			TableOnly,
			nil

	case 2:
		return NewTable(
				parts[0],
				Public,
				parts[1]),
			TableAndDatabase,
			nil

	case 3:
		return NewTable(
				parts[0],
				parts[1],
				parts[2]),
			FullyQualified,
			nil

	default:
		return Table{}, 0, errors.New("too many name parts in input")
	}
}
