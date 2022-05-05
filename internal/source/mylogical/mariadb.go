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

// mariadbStamp adapts mysql.MariadbGTIDSet, so it implements the required String and Less methods.
type mariadbStamp struct {
	gtidset *mysql.MariadbGTIDSet
}

var (
	_ stamp.Stamp = newMariadbStamp()
)

func newMariadbStamp() mariadbStamp {
	gtidset := new(mysql.MariadbGTIDSet)
	gtidset.Sets = make(map[uint32]*mysql.MariadbGTID)
	return mariadbStamp{
		gtidset: gtidset,
	}
}
func (s mariadbStamp) MarshalText() (text []byte, err error) {
	if s.gtidset == nil {
		return []byte(""), nil
	}
	return []byte(s.gtidset.String()), nil
}
func (s mariadbStamp) Less(other stamp.Stamp) bool {
	if o, ok := other.(mariadbStamp); ok {
		if o.gtidset == nil {
			return false
		}
		if s.gtidset == nil {
			return true
		}
		return o.gtidset.Contain(s.gtidset) && !s.gtidset.Equal(o.gtidset)
	}
	return false
}

func (s mariadbStamp) addMariaGTIDSet(a *mysql.MariadbGTID) mariadbStamp {
	if clone, ok := s.gtidset.Clone().(*mysql.MariadbGTIDSet); ok {
		clone.AddSet(a)
		return mariadbStamp{gtidset: clone}
	}
	return s
}
