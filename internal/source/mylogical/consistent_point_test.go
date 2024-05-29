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

// Package mylogical contains support for reading a mySQL logical
// replication feed.
// It uses Replication with Global Transaction Identifiers.
// See  https://dev.mysql.com/doc/refman/8.0/en/replication-gtids.html
package mylogical

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClone(t *testing.T) {
	tests := []struct {
		name   string
		flavor string
		this   string
	}{
		{"empty", mysql.MySQLFlavor, ""},
		{"empty", mysql.MariaDBFlavor, ""},
		{"single", mysql.MySQLFlavor, "6fa7e6ef-c49a-11ec-950a-0242ac120002:1-5374"},
		{"single", mysql.MariaDBFlavor, "1-1-1"},
		{"multi", mysql.MySQLFlavor, "a31203a1-0f26-425d-be1a-86d23f37d87f:1-10,6fa7e6ef-c49a-11ec-950a-0242ac120002:1-20"},
		{"multi", mysql.MariaDBFlavor, "1-1-1,2-2-2"},
	}
	for _, tt := range tests {
		name := fmt.Sprintf("%s-%s", tt.name, tt.flavor)
		t.Run(name, func(t *testing.T) {
			a := assert.New(t)
			r := require.New(t)
			this, err := newConsistentPoint(tt.flavor).parseFrom(tt.this)
			r.NoError(err)
			other := this.clone()
			a.Equal(this, other)
			a.NotSame(this, other)
		})
	}
	t.Run("panic", func(t *testing.T) {
		assert.Panics(t, func() {
			cp := &consistentPoint{}
			cp.clone()
		}, "consistentPoint.clone did not panic")
	})
}

func TestMySqlStampLess(t *testing.T) {
	tests := []struct {
		name string
		this string
		that string
		want bool
	}{
		{"empty0", "", "", false},
		{"empty1", "", "6fa7e6ef-c49a-11ec-950a-0242ac120002:1-5374", true},
		{"empty2", "6fa7e6ef-c49a-11ec-950a-0242ac120002:1-5374", "", false},
		{"single0", "6fa7e6ef-c49a-11ec-950a-0242ac120002:1-5374", "6fa7e6ef-c49a-11ec-950a-0242ac120002:1-5374", false},
		{"single1", "6fa7e6ef-c49a-11ec-950a-0242ac120002:1-5373", "6fa7e6ef-c49a-11ec-950a-0242ac120002:1-5374", true},
		{"single2", "6fa7e6ef-c49a-11ec-950a-0242ac120002:1-5375", "6fa7e6ef-c49a-11ec-950a-0242ac120002:1-5374", false},
		{"multi0", "a31203a1-0f26-425d-be1a-86d23f37d87f:1-10,6fa7e6ef-c49a-11ec-950a-0242ac120002:1-19",
			"a31203a1-0f26-425d-be1a-86d23f37d87f:1-10,6fa7e6ef-c49a-11ec-950a-0242ac120002:1-20", true},
		{"multi0", "a31203a1-0f26-425d-be1a-86d23f37d87f:1-10,6fa7e6ef-c49a-11ec-950a-0242ac120002:1-20",
			"a31203a1-0f26-425d-be1a-86d23f37d87f:1-11,6fa7e6ef-c49a-11ec-950a-0242ac120002:1-20", true},
		{"multi0", "a31203a1-0f26-425d-be1a-86d23f37d87f:1-11",
			"a31203a1-0f26-425d-be1a-86d23f37d87f:1-10,6fa7e6ef-c49a-11ec-950a-0242ac120002:1-20", false},
		{"multi0", "a31203a1-0f26-425d-be1a-86d23f37d87f:1-10",
			"a31203a1-0f26-425d-be1a-86d23f37d87f:1-11,6fa7e6ef-c49a-11ec-950a-0242ac120002:1-20", true},
		{"disjoint0", "a31203a1-0f26-425d-be1a-86d23f37d87f:1-10:12-13",
			"a31203a1-0f26-425d-be1a-86d23f37d87f:1-13,6fa7e6ef-c49a-11ec-950a-0242ac120002:1-20", true},
		{"disjoint1", "a31203a1-0f26-425d-be1a-86d23f37d87f:1-13",
			"a31203a1-0f26-425d-be1a-86d23f37d87f:1-10:12-13,6fa7e6ef-c49a-11ec-950a-0242ac120002:1-20", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := assert.New(t)
			this, err := newConsistentPoint(mysql.MySQLFlavor).parseFrom(tt.this)
			if !a.NoError(err) {
				return
			}
			that, err := newConsistentPoint(mysql.MySQLFlavor).parseFrom(tt.that)
			if !a.NoError(err) {
				return
			}
			a.Equalf(tt.want, this.Less(that), "%s failed", tt.name)
			checkMarshal(a, mysql.MySQLFlavor, this)
			checkMarshal(a, mysql.MySQLFlavor, that)
		})
	}
}

func TestMariadbStampLess(t *testing.T) {
	tests := []struct {
		name string
		this string
		that string
		want bool
	}{
		{"empty0", "", "", false},
		{"empty1", "", "1-1-1", true},
		{"empty2", "1-1-1", "", false},
		{"single0", "1-1-1", "1-1-1", false},
		{"single1", "1-1-1", "1-1-2", true},
		{"single2", "1-1-2", "1-1-1", false},
		{"multi0", "1-1-1,2-2-2", "1-1-1,2-2-3", true},
		{"multi1", "1-1-1,2-2-2", "1-1-2,2-2-2", true},
		{"multi2", "1-1-1,2-2-2", "1-1-1,2-2-2", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := assert.New(t)
			this, err := newConsistentPoint(mysql.MariaDBFlavor).parseFrom(tt.this)
			if !a.NoError(err) {
				return
			}
			that, err := newConsistentPoint(mysql.MariaDBFlavor).parseFrom(tt.that)
			if !a.NoError(err) {
				return
			}
			a.Equalf(tt.want, this.Less(that), "%s failed", tt.name)
			checkMarshal(a, mysql.MariaDBFlavor, this)
			checkMarshal(a, mysql.MariaDBFlavor, that)
		})
	}
}

type interval struct {
	from int
	to   int
}

func TestWithMysqlGTIDSet(t *testing.T) {

	gtid := func(intervals []interval) string {
		buff := strings.Builder{}

		buff.WriteString("6fa7e6ef-c49a-11ec-950a-0242ac120002")
		for _, interval := range intervals {
			buff.WriteString(fmt.Sprintf(":%d-%d", interval.from, interval.to))
		}
		return buff.String()
	}
	tests := []struct {
		name    string
		initial string
		add     string
		want    string
	}{
		{"from empty", "", gtid([]interval{{1, 10}}), gtid([]interval{{1, 10}})},
		{"noop", gtid([]interval{{1, 10}}), gtid([]interval{{1, 10}}), gtid([]interval{{1, 10}})},
		{"contiguous", gtid([]interval{{1, 10}}), gtid([]interval{{11, 20}}), gtid([]interval{{1, 20}})},
		{"disjoint", gtid([]interval{{1, 10}}), gtid([]interval{{12, 20}}), gtid([]interval{{1, 10}, {12, 20}})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := require.New(t)
			a := assert.New(t)
			this, err := newConsistentPoint(mysql.MySQLFlavor).parseFrom(tt.initial)
			r.NoError(err)
			toAdd, err := mysql.ParseUUIDSet(tt.add)
			r.NoError(err)
			res := this.withMysqlGTIDSet(time.Now(), toAdd)

			a.Equal(tt.want, res.my.Sets["6fa7e6ef-c49a-11ec-950a-0242ac120002"].String())
		})
	}

}
func checkMarshal(a *assert.Assertions, flavor string, cp *consistentPoint) {
	data, err := cp.MarshalJSON()
	if !a.NoError(err) {
		return
	}
	next := newConsistentPoint(flavor)
	if !a.NoError(next.UnmarshalJSON(data)) {
		return
	}
	a.Equal(cp, next)
}
