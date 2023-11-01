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

package schemawatch

// This file is where we can add hacks to interpret incoming JSON into
// whatever data types makes sense for further processing. In general,
// we expect CRDB -> CRDB to be a no-op, since CockroachDB can interpret
// its own output.
//
// Where we do need extra support is tweaking the datatypes used for
// time when sending to Oracle.  At present, it appears that we need to
// return time.Time as the specialized driver types in order to get
// correct treatment of timezones.

import (
	"encoding/hex"
	"encoding/json"
	"regexp"
	"strconv"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	ora "github.com/sijms/go-ora/v2"
)

var (
	// These are evaluated in order.
	oraParseHelpers = []struct {
		pattern *regexp.Regexp
		parser  func(any) (any, error)
	}{
		{
			// This is a special-case for UUIDs in the source being stored
			// as a 16-byte raw value in the destination.
			pattern: regexp.MustCompile(`^RAW\(16\)$`),
			parser: func(a any) (any, error) {
				s, ok := a.(string)
				if !ok {
					return nil, errors.Errorf("expecting string, got %T", a)
				}
				u, err := uuid.Parse(s)
				return u[:], err
			},
		},
		{
			pattern: regexp.MustCompile(`^TIMESTAMP\(\d+\) WITH TIME ZONE$`),
			parser: func(a any) (any, error) {
				s, ok := a.(string)
				if !ok {
					return nil, errors.Errorf("expecting string, got %T", a)
				}
				t, err := time.ParseInLocation(time.RFC3339Nano, s, time.UTC)
				return ora.TimeStampTZ(t), err
			},
		},
		{
			// Try parsing with and without a timezone specifier.
			pattern: regexp.MustCompile(`^TIMESTAMP\(\d+\)$`),
			parser: func(a any) (any, error) {
				s, ok := a.(string)
				if !ok {
					return nil, errors.Errorf("expecting string, got %T", a)
				}
				if t, err := time.ParseInLocation(time.RFC3339Nano, s, time.UTC); err == nil {
					return ora.TimeStamp(t), nil
				}
				t, err := time.ParseInLocation("2006-01-02T15:04:05", s, time.UTC)
				return ora.TimeStamp(t), err
			},
		},
		{
			pattern: regexp.MustCompile(`^DATE$`),
			parser: func(a any) (any, error) {
				s, ok := a.(string)
				if !ok {
					return nil, errors.Errorf("expecting string, got %T", a)
				}
				t, err := time.ParseInLocation("2006-01-02", s, time.UTC)
				return ora.TimeStamp(t), err
			},
		},
	}
)

// coerce a bit-string into a integer
func coerceInt(a any) (any, error) {
	s, ok := a.(string)
	if !ok {
		return nil, errors.Errorf("expecting a string, got %T", a)
	}
	return strconv.ParseInt(s, 2, 64)
}

// coerce a hex-string to its binary representation, after stripping the "\x" prefix
func coerceHexString(a any) (any, error) {
	s, ok := a.(string)
	if !ok || len(s) < 2 {
		return nil, errors.Errorf("expecting a hex encoded string, got %T", a)
	}
	return hex.DecodeString(s[2:])
}

// coerce to json
func coerceJSON(a any) (any, error) {
	return json.Marshal(a)
}

// reifyJSON converts an incoming byte array to a reified type.
func reifyJSON(v any) (any, error) {
	if buf, ok := v.([]byte); ok {
		if err := json.Unmarshal(buf, &v); err != nil {
			return nil, err
		}
	}
	return v, nil
}

func parseHelper(product types.Product, typeName string) func(any) (any, error) {
	switch product {
	case types.ProductCockroachDB, types.ProductPostgreSQL:
		switch typeName {
		case "JSON", "JSONB":
			// Ensure that data bound for a JSON column is reified.
			return reifyJSON
		default:
			return nil
		}
	case types.ProductMariaDB, types.ProductMySQL:
		// Coerce types to the what the mysql driver expects.
		switch typeName {
		case "binary", "blob", "longblob", "mediumblob", "tinyblob", "varbinary":
			return coerceHexString
		case "bit":
			return coerceInt
		case "json", "geometry", "geography":
			return coerceJSON
		default:
			return nil
		}
	case types.ProductOracle:
		for _, helper := range oraParseHelpers {
			if helper.pattern.MatchString(typeName) {
				return helper.parser
			}
		}
	}
	return nil
}
