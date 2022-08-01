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
	"github.com/pkg/errors"
)

// consistentPoint provides a uniform API around the various
// flavors of GTIDSet used by the replication library.
type consistentPoint struct {
	ma *mysql.MariadbGTIDSet
	my *mysql.MysqlGTIDSet
}

var _ stamp.Stamp = (*consistentPoint)(nil)

// newConsistentPoint constructs an empty consistentPoint for the
// specified flavor.
func newConsistentPoint(flavor string) (*consistentPoint, error) {
	switch flavor {
	case mysql.MariaDBFlavor:
		return &consistentPoint{
			ma: &mysql.MariadbGTIDSet{
				Sets: make(map[uint32]*mysql.MariadbGTID),
			},
		}, nil

	case mysql.MySQLFlavor:
		return &consistentPoint{
			my: &mysql.MysqlGTIDSet{
				Sets: make(map[string]*mysql.UUIDSet),
			},
		}, nil

	default:
		return nil, errors.Errorf("Invalid flavor  %s", flavor)
	}
}

// AsGTIDSet returns the enclosed GTIDSet.
func (c *consistentPoint) AsGTIDSet() mysql.GTIDSet {
	switch {
	case c.ma != nil:
		return c.ma
	case c.my != nil:
		return c.my
	default:
		return nil
	}
}

// IsZero returns true if the consistentPoint contains no GTID values.
func (c *consistentPoint) IsZero() bool {
	switch {
	case c.ma != nil:
		return len(c.ma.Sets) == 0
	case c.my != nil:
		return len(c.my.Sets) == 0
	default:
		return true
	}
}

// Less implements stamp.Stamp.
func (c *consistentPoint) Less(other stamp.Stamp) bool {
	oPoint := other.(*consistentPoint)
	if oPoint.IsZero() {
		return false
	}
	if c.IsZero() {
		return true
	}

	cSet := c.AsGTIDSet()
	oSet := oPoint.AsGTIDSet()
	return oSet.Contain(cSet) && !cSet.Equal(oSet)
}

func (c *consistentPoint) MarshalText() (text []byte, err error) {
	return []byte(c.AsGTIDSet().String()), nil
}

// String is for debugging use only.
func (c *consistentPoint) String() string {
	return c.AsGTIDSet().String()
}

// UnmarshalText decodes GTID Sets expressed as strings.
// Supports MySQL or MariaDB
// See https://dev.mysql.com/doc/refman/8.0/en/replication-gtids-concepts.html
// and https://mariadb.com/kb/en/gtid/
// Examples:
// MySQL: E11FA47-71CA-11E1-9E33-C80AA9429562:1-3:11:47-49
// MariaDB: 0-1-1
func (c *consistentPoint) UnmarshalText(text []byte) error {
	switch {
	case c.ma != nil:
		set, err := mysql.ParseMariadbGTIDSet(string(text))
		if err != nil {
			return err
		}
		c.ma = set.(*mysql.MariadbGTIDSet)

	case c.my != nil:
		set, err := mysql.ParseMysqlGTIDSet(string(text))
		if err != nil {
			return err
		}
		c.my = set.(*mysql.MysqlGTIDSet)

	default:
		return errors.New("no flavor configured")
	}
	return nil
}

func (c *consistentPoint) withMariaGTIDSet(set *mysql.MariadbGTID) (*consistentPoint, error) {
	cloned := c.ma.Clone().(*mysql.MariadbGTIDSet)
	if err := cloned.AddSet(set); err != nil {
		return c, err
	}
	return &consistentPoint{ma: cloned}, nil
}

func (c *consistentPoint) withMysqlGTIDSet(set *mysql.UUIDSet) *consistentPoint {
	cloned := c.my.Clone().(*mysql.MysqlGTIDSet)
	cloned.AddSet(set)
	return &consistentPoint{my: cloned}
}
