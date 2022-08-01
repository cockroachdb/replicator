// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package mylogical contains support for reading a mySQL logical
// replication feed.
// It uses Replication with Global Transaction Identifiers.
// See  https://dev.mysql.com/doc/refman/8.0/en/replication-gtids.html
package mylogical

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/stretchr/testify/assert"
)

func Test_mySqlStamp_Less(t *testing.T) {
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

func Test_mariadbStamp_Less(t *testing.T) {
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
