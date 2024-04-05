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

package ident

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHint(t *testing.T) {
	r := require.New(t)

	tbl := NewTable(MustSchema(New("my_db"), Public), New("tbl"))
	h := WithHint(tbl, "@{HINT}")

	r.Equal(`my_db.public.tbl@{HINT}`, h.Raw())
	r.Equal(`"my_db"."public"."tbl"@{HINT}`, h.String())
}
