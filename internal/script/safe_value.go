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

package script

import (
	"bytes"
	"encoding/json"
	"strconv"
	"time"

	"github.com/dop251/goja"
	"github.com/pkg/errors"
)

// The maximum safe numeric value in JavaScript.
const maxInt = 1 << 53

// safeValue returns a JS runtime value that contains the given value
// as though it had been stringified and then parsed.
//
// Numbers will be converted to a string representation to ensure
// minimum loss of fidelity when round-tripped through the userscript.
// If the user actually wants to perform math in JavaScript, the Number
// API is available, or the JS idiom of `+value` can be used.
func safeValue(rt *goja.Runtime, value any) (goja.Value, error) {
	// This should cover the 80% use case of values returned from a
	// database query.
	switch t := value.(type) {
	case goja.Value:
		return t, nil

	case nil:
		return goja.Null(), nil

	case bool, string:
		return rt.ToValue(t), nil

	case int:
		return rt.ToValue(strconv.FormatInt(int64(t), 10)), nil
	case int8:
		return rt.ToValue(strconv.FormatInt(int64(t), 10)), nil
	case int16:
		return rt.ToValue(strconv.FormatInt(int64(t), 10)), nil
	case int32:
		return rt.ToValue(strconv.FormatInt(int64(t), 10)), nil
	case int64:
		return rt.ToValue(strconv.FormatInt(t, 10)), nil

	case uint:
		return rt.ToValue(strconv.FormatUint(uint64(t), 10)), nil
	case uint8:
		return rt.ToValue(strconv.FormatUint(uint64(t), 10)), nil
	case uint16:
		return rt.ToValue(strconv.FormatUint(uint64(t), 10)), nil
	case uint32:
		return rt.ToValue(strconv.FormatUint(uint64(t), 10)), nil
	case uint64:
		return rt.ToValue(strconv.FormatUint(t, 10)), nil

	case float32:
		return rt.ToValue(strconv.FormatFloat(float64(t), 'f', -1, 32)), nil
	case float64:
		return rt.ToValue(strconv.FormatFloat(t, 'f', -1, 64)), nil

	case time.Time:
		return rt.ToValue(t.UTC().Format(time.RFC3339Nano)), nil
	}

	// Marshal other random types, perhaps json.RawMessage.
	buf, err := json.Marshal(value)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// Use our number-preserving unmarshal function.
	switch buf[0] {
	case '{':
		m := make(map[string]any)
		if err := unmarshal(buf, &m); err != nil {
			return nil, errors.WithStack(err)
		}
		return rt.ToValue(m), nil

	case '[':
		var arr []any
		if err := unmarshal(buf, &arr); err != nil {
			return nil, errors.WithStack(err)
		}
		return rt.ToValue(arr), nil

	case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '-':
		var n json.Number
		if err := unmarshal(buf, &n); err != nil {
			return nil, errors.WithStack(err)
		}
		return rt.ToValue(n.String()), nil

	case 'f':
		return rt.ToValue(false), nil

	case 't':
		return rt.ToValue(true), nil

	default:
		// We're going to call the JSON.parse function within the
		// runtime. This seems to be the least-worst way to re-parse
		// strings and any quoted characters therein.
		jsonObj := rt.GlobalObject().Get("JSON").(*goja.Object)
		parse := jsonObj.Get("parse").Export().(func(call goja.FunctionCall) goja.Value)
		return parse(goja.FunctionCall{
			Arguments: []goja.Value{
				rt.ToValue(string(buf)),
			},
		}), nil
	}
}

// unmarshal should be used instead of [json.Unmarshal] to ensure that
// numeric types are decoded with minimum loss of precision.
func unmarshal(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	return dec.Decode(v)
}
