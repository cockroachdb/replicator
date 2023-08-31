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

	"github.com/stretchr/testify/assert"
)

func TestUDTJson(t *testing.T) {
	a := assert.New(t)

	id := NewUDT(MustSchema(New("db"), New("schema")), New("my_enum"))
	data, err := json.Marshal(id)
	a.NoError(err)

	var id2 UDT
	a.NoError(json.Unmarshal(data, &id2))
	a.Equal(id, id2)
	a.Same(id.namespace, id2.namespace)
	a.Same(id.terminal, id2.terminal)
}

func TestUDTArrayJson(t *testing.T) {
	a := assert.New(t)

	id := NewUDTArray(MustSchema(New("db"), New("schema")), New("my_enum"))
	data, err := json.Marshal(id)
	a.NoError(err)

	var id2 UDT
	a.NoError(json.Unmarshal(data, &id2))
	a.Equal(id, id2)
	a.True(id2.IsArray())
	a.Same(id.namespace, id2.namespace)
	a.Same(id.terminal, id2.terminal)
}
