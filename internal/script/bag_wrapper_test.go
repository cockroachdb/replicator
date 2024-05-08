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
	"testing"
	"time"

	"github.com/cockroachdb/replicator/internal/util/merge"
	"github.com/dop251/goja"
	"github.com/stretchr/testify/require"
)

func TestBagWrapper(t *testing.T) {
	r := require.New(t)
	rt := goja.New()

	p := struct {
		Bar  int
		Foo  string
		Time time.Time
	}{
		Bar:  1,
		Foo:  `"1"`,
		Time: time.UnixMilli(1708731562135).UTC(),
	}

	wrapper := &bagWrapper{
		data: merge.NewBagOf(nil, nil, "obj", &p, "simple", 42),
		rt:   rt,
	}
	r.False(wrapper.Has("nope"))
	r.True(wrapper.Has("obj"))

	keys := wrapper.Keys()
	r.Len(keys, 2)
	r.Contains(keys, "obj")
	r.Contains(keys, "simple")

	obj := wrapper.Get("obj").(*goja.Object)
	r.Equal(int64(1), obj.Get("Bar").ToInteger())
	r.Equal(`"1"`, obj.Get("Foo").String())

	simple := wrapper.Get("simple").ToInteger()
	r.Equal(int64(42), simple)

	ts := obj.Get("Time").String()
	r.Equal("2024-02-23T23:39:22.135Z", ts)
	r.Equal("2024-02-23 23:39:22.135 +0000 UTC", p.Time.String())

	wrapper.Delete("obj")
	r.Equal(goja.Undefined(), wrapper.Get("obj"))

	wrapper.Set("another", rt.ToValue("value"))
	r.Equal("value", wrapper.Get("another").String())
}
