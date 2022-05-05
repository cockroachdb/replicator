// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package mylogical

import (
	"github.com/cockroachdb/cdc-sink/internal/util/stamp"
	"github.com/go-mysql-org/go-mysql/mysql"
)

// mySQLStamp adapts mysql.MysqlGTIDSet, so it implements the required MarshalText and Less methods.
type mySQLStamp struct {
	gtidset *mysql.MysqlGTIDSet
}

var (
	_ stamp.Stamp = newMySQLStamp()
)

func newMySQLStamp() mySQLStamp {
	gtidset := new(mysql.MysqlGTIDSet)
	gtidset.Sets = make(map[string]*mysql.UUIDSet)
	return mySQLStamp{
		gtidset: gtidset,
	}
}
func (s mySQLStamp) MarshalText() (text []byte, err error) {
	if s.gtidset == nil {
		return nil, nil
	}
	return []byte(s.gtidset.String()), nil
}
func (s mySQLStamp) Less(other stamp.Stamp) bool {
	o := other.(mySQLStamp)
	if o.gtidset == nil {
		return false
	}
	if s.gtidset == nil {
		return true
	}
	return o.gtidset.Contain(s.gtidset) && !s.gtidset.Equal(o.gtidset)
}

func (s mySQLStamp) addMysqlGTIDSet(a *mysql.UUIDSet) mySQLStamp {
	if clone, ok := s.gtidset.Clone().(*mysql.MysqlGTIDSet); ok {
		clone.AddSet(a)
		return mySQLStamp{gtidset: clone}
	}
	// this should not happen.
	panic("gtidset inside mySQLStamp must be a *mysql.UUIDSet")
}
