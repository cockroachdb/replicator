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

// Qualification is a return value from ParseTableRelative, indicating how many
// name parts were present in the initial input.
type Qualification int

//go:generate go run golang.org/x/tools/cmd/stringer -type=Qualification

// Various levels of table-identifier qualification.
const (
	TableOnly Qualification = iota + 1
	PartialSchema
	FullyQualified
)

// ParseSchema parses a dot-separated schema name.
func ParseSchema(s string) (Schema, error) {
	parts, err := parseDottedIdent(s)
	if err != nil {
		return Schema{}, errors.Wrapf(err, "could not parse %q as a schema name", s)
	}
	return NewSchema(parts...)
}

// ParseTable parses a table name.
func ParseTable(s string) (Table, error) {
	parts, err := parseDottedIdent(s)
	if err != nil {
		return Table{}, errors.Wrapf(err, "could not parse %q as a table name", s)
	}
	if len(parts) == 0 {
		return Table{}, nil
	}

	sch, err := NewSchema(parts[:len(parts)-1]...)
	if err != nil {
		return Table{}, errors.Wrapf(err, "could not parse %q as a table name", s)
	}
	last := parts[len(parts)-1]

	return Table{qualified: qualifieds.Get(qualifiedKey{
		namespace: sch.array,
		terminal:  last.atom,
	})}, nil
}

// ParseTableRelative parses a table name, relative to a base schema.
// The string must have no more than the number of name parts in
// relativeTo, plus one for the table name itself.
func ParseTableRelative(s string, relativeTo Schema) (Table, Qualification, error) {
	parts, err := parseDottedIdent(s)
	if err != nil {
		return Table{}, 0, errors.Wrapf(err, "could not parse %q as a table name", s)
	}

	if len(parts) == 0 {
		return Table{}, 0, errors.New("table name is empty, please ensure the table name is included in the path")
	}

	// Typical case, only a table name was specified.
	if len(parts) == 1 {
		return NewTable(relativeTo, parts[0]), TableOnly, nil
	}

	// Save off the last part as the table name.
	tableIdent := parts[len(parts)-1]
	nextSchema, qual, err := relativeTo.Relative(parts[:len(parts)-1]...)
	if err != nil {
		return Table{}, 0, err
	}
	return NewTable(nextSchema, tableIdent), qual, nil
}

func parseDottedIdent(s string) ([]Ident, error) {
	parts := make([]Ident, 0, maxArrayLength+1)
	for s != "" {
		var part Ident

		part, remaining, err := ParseIdent(s)
		if err != nil {
			return nil, err
		}
		parts = append(parts, part)

		// Skip next dot, if any.
		if remaining == "" {
			break
		}
		if rune(remaining[0]) != separator {
			return nil, errors.New("expecting separator")
		}
		s = remaining[1:]
	}
	return parts, nil
}
