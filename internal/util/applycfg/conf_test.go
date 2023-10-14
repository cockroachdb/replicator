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

package applycfg

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/merge"
	"github.com/stretchr/testify/assert"
)

func TestCopyEquals(t *testing.T) {
	a := assert.New(t)

	cfg := &Config{
		CASColumns: TargetColumns{ident.New("cas")},
		Deadlines:  ident.MapOf[time.Duration](ident.New("dl"), time.Hour),
		Exprs:      ident.MapOf[string]("expr", "foo"),
		Extras:     ident.New("extras"),
		Ignore:     ident.MapOf[bool]("ign", true),
		Merger: merge.Func(func(context.Context, *merge.Conflict) (*merge.Resolution, error) {
			panic("unused")
		}),
		SourceNames: ident.MapOf[SourceColumn](ident.New("new"), ident.New("old")),
	}

	a.True(cfg.Equal(cfg))

	cpy := cfg.Copy()
	a.NotSame(cfg, cpy)
	a.True(cfg.Equal(cpy))

	patched := NewConfig().Patch(cpy)
	a.NotSame(cpy, patched)
	a.True(cfg.Equal(patched))
}

func TestZero(t *testing.T) {
	a := assert.New(t)

	a.True(NewConfig().IsZero())
}
