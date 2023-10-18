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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMap(t *testing.T) {
	r := require.New(t)

	m := MapOf[string](
		Ident{}, "empty",
		"foo", "foo",
		"FOO", "FOO",
	)

	r.Equal(2, m.Len())

	found, ok := m.Get(Ident{})
	r.True(ok)
	r.Equal("empty", found)

	found, ok = m.Get(New("Foo"))
	r.True(ok)
	r.Equal("FOO", found)
}

// Ensure that a nil in the input to [MapOf] translates correctly into a
// zero value for the type.
func TestMapOfNil(t *testing.T) {
	r := require.New(t)

	m := MapOf[string](Ident{}, nil)
	r.Equal(1, m.Len())
	v, ok := m.Get(Ident{})
	r.True(ok)
	r.Equal("", v)
}

func TestMapJSON(t *testing.T) {
	r := require.New(t)

	m := MapOf[Ident](
		"", New("empty"),
		"foo", New("foo"),
		"FOO", New("FOO"),
	)

	buf, err := json.Marshal(&m)
	r.NoError(err)

	t.Log(string(buf))

	var next Map[Ident]
	r.NoError(json.Unmarshal(buf, &next))

	r.Equal(2, next.Len())
}
