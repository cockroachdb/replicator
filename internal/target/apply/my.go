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

package apply

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cdc-sink/internal/util/ident"
)

func myIdentifier(id ident.Identifier) string {
	var parts []ident.Ident
	var sb strings.Builder
	parts = id.Idents(parts)
	for idx, part := range parts {
		if idx > 0 {
			sb.WriteRune('.')
		}
		sb.WriteString(fmt.Sprintf("`%s`", part.Raw()))
	}
	return sb.String()
}
