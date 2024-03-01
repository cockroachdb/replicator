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

// Package crep ("see-rep") contains a utility for producing a Canonical
// REPresentation of a value type.
package crep

import (
	"bytes"
	"encoding/json"
	"io"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

// A Value is one of:
//   - nil
//   - string
//   - bool
//   - []Value
//   - map[string]Value
type Value any

// Canonical returns a canonical, JSON-esque representation of an input
// value. The values returned by this function will, in general, agree
// with the changefeed encoding for that value type. Furthermore,
// numbers will be converted to a string representation, so that they
// may be presented to the script runtime without loss of precision.
func Canonical(value any) (Value, error) {
	// This should cover the 80% use case of values returned from a
	// database query.
	var buf []byte
	switch t := value.(type) {

	case nil:
		return nil, nil

	case bool, string:
		return t, nil

	case int:
		return strconv.FormatInt(int64(t), 10), nil
	case int8:
		return strconv.FormatInt(int64(t), 10), nil
	case int16:
		return strconv.FormatInt(int64(t), 10), nil
	case int32:
		return strconv.FormatInt(int64(t), 10), nil
	case int64:
		return strconv.FormatInt(t, 10), nil

	case uint:
		return strconv.FormatUint(uint64(t), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(t), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(t), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(t), 10), nil
	case uint64:
		return strconv.FormatUint(t, 10), nil

	case float32:
		return strconv.FormatFloat(float64(t), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64), nil

	case time.Time:
		// Very common case and we can encode to our desired format.
		return t.UTC().Format(time.RFC3339Nano), nil

	case json.RawMessage:
		// Parse raw message as-is.
		buf = t

	case []byte:
		// Parse raw message as-is.
		buf = t

	default:
		// Marshal other random types.
		var err error
		buf, err = json.Marshal(t)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}

	return Unmarshal(buf)
}

// Unmarshal should be used instead of [json.Unmarshal] to ensure that
// numeric types are decoded with minimum loss of precision.
func Unmarshal(data []byte) (Value, error) {
	reader := bytes.NewReader(data)
	dec := json.NewDecoder(reader)
	dec.UseNumber()

	ret, err := (&decoder{source: dec}).Decode()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if dec.More() {
		return nil, errors.New("invalid JSON input")
	}
	return ret, nil
}

// decoder interprets a JSON token stream, replacing any [json.Number]
// values that we encounter with a string.
type decoder struct {
	source *json.Decoder

	arrStack []*[]any
	objStack []*map[string]any
	valStack []any
}

// Decode returns a reified form of the next value in the JSON token
// stream.
func (d *decoder) Decode() (any, error) {
	for {
		token, err := d.source.Token()
		if errors.Is(err, io.EOF) {
			return nil, errors.New("unexpected EOF")
		} else if err != nil {
			return nil, errors.Wrapf(err, "offset %d", d.source.InputOffset())
		}

		// Simple values are pushed directly onto the value stack.
		switch t := token.(type) {
		case nil:
			d.valStack = append(d.valStack, nil)
		case bool:
			d.valStack = append(d.valStack, t)
		case string:
			d.valStack = append(d.valStack, t)
		case json.Number:
			d.valStack = append(d.valStack, t.String())
		case float64:
			return nil, errors.Errorf("call UseNumber() on underlying decoder")

		case json.Delim:
			// Token will have already handled delimiter matching, so
			// we're guaranteed to get structurally sound JSON input.
			switch t {
			case '[':
				// When we see an opening delimiter, we'll push a
				// pointer to the enclosing accumulator (a map or a
				// slice) onto both the value stack and a separate
				// accumulator-specific stack.
				var arr []any
				ptr := &arr
				d.arrStack = append(d.arrStack, ptr)
				d.valStack = append(d.valStack, ptr)
				continue

			case '{':
				// As above. While a map is a pointer type, it is not a
				// comparable type, so we still need a pointer to know
				// when we're done popping values.
				obj := make(map[string]any)
				ptr := &obj
				d.objStack = append(d.objStack, ptr)
				d.valStack = append(d.valStack, ptr)
				continue

			case ']':
				// When we see a closing delimiter, we'll pop the
				// relevant pointer from the type-specific accumulator
				// stack. We'll consume elements from the value stack
				// until encountering the accumulator on the value
				// stack. Once we see the accumulator on the value
				// stack, we know we can stop consuming values.
				accumulator := d.popArrPtr()
			accumulateArr:
				for {
					if ptr, ok := d.topValMayBePointer().(*[]any); ok && accumulator == ptr {
						// We're accumulating the values in a LIFO
						// order, so we need to reverse the slice. We'll
						// do this by walking from both ends until the
						// two indexes meet in the middle. This also
						// handles the empty or singleton case since j
						// will either be -1 or 0, respectively, which
						// is not greater than i=0.
						for i, j := 0, len(*accumulator)-1; i < j; i, j = i+1, j-1 {
							(*accumulator)[i], (*accumulator)[j] =
								(*accumulator)[j], (*accumulator)[i]
						}
						break accumulateArr
					}
					*accumulator = append(*accumulator, d.popValActual())
				}

			case '}':
				// As above, although we don't need to worry about the
				// order of keys in a map.
				accumulator := d.popObjPtr()
			accumulateObj:
				for {
					if m, ok := d.topValMayBePointer().(*map[string]any); ok && accumulator == m {
						break accumulateObj
					}
					val := d.popValActual()
					key := d.popValActual().(string)
					(*accumulator)[key] = val
				}

			default:
				return nil, errors.Errorf("unexpected delimiter: %s", string(t))
			}

		default:
			return nil, errors.Errorf("unexpected token type %T", t)
		}

		if len(d.valStack) == 1 {
			return d.popValActual(), nil
		}
	}
}

// popArrPtr pops the top array accumulator pointer from the stack.
func (d *decoder) popArrPtr() *[]any {
	idx := len(d.arrStack) - 1
	ret := d.arrStack[idx]
	d.arrStack = d.arrStack[:idx]
	return ret
}

// popObjPtr pops the top object accumulator pointer from the stack.
func (d *decoder) popObjPtr() *map[string]any {
	idx := len(d.objStack) - 1
	ret := d.objStack[idx]
	d.objStack = d.objStack[:idx]
	return ret
}

// popValActual pops the top value from the value stack. If the top
// value is a pointer to an accumulator (slice or map), we'll
// dereference the pointer and return the underlying accumulator.
func (d *decoder) popValActual() any {
	idx := len(d.valStack) - 1
	ret := d.valStack[idx]
	d.valStack = d.valStack[:idx]
	switch t := ret.(type) {
	case *[]any:
		return *t
	case *map[string]any:
		return *t
	default:
		return t
	}
}

// topValMayBePointer returns the top-most value in the value stack.
// This might be a pointer to an accumulator, so the value is not useful
// other than to see if we're about to exit a collection type.
func (d *decoder) topValMayBePointer() any {
	return d.valStack[len(d.valStack)-1]
}
