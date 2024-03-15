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

package sequencer

import (
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/stretchr/testify/require"
)

func TestCommonProgress(t *testing.T) {
	r := require.New(t)

	schema := ident.MustSchema(ident.New("foo"), ident.New("bar"))
	group := &types.TableGroup{
		Tables: []ident.Table{
			ident.NewTable(schema, ident.New("t1")),
			ident.NewTable(schema, ident.New("t2")),
		},
	}

	progress := &ident.TableMap[hlc.Range]{}
	stat := NewStat(group, progress)

	// Nil case.
	r.Equal(hlc.RangeEmpty(), CommonProgress(nil))

	// Empty case.
	r.Equal(hlc.RangeEmpty(), CommonProgress(stat))

	// One table with no progress.
	progress.Put(group.Tables[0], hlc.RangeIncluding(hlc.Zero(), hlc.New(1, 1)))
	r.Equal(hlc.RangeEmpty(), CommonProgress(stat))

	// All tables have progress.
	progress.Put(group.Tables[1], hlc.RangeIncluding(hlc.Zero(), hlc.New(2, 1)))
	r.Equal(hlc.RangeIncluding(hlc.Zero(), hlc.New(1, 1)), CommonProgress(stat))
}
