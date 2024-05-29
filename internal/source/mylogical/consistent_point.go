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

package mylogical

import (
	"encoding/json"
	"time"

	"github.com/cockroachdb/replicator/internal/util/stamp"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pkg/errors"
)

// consistentPoint provides a uniform API around the various
// flavors of GTIDSet used by the replication library.
type consistentPoint struct {
	ma *mysql.MariadbGTIDSet
	my *mysql.MysqlGTIDSet
	ts time.Time // The approximate wall time of the consistent point.
}

// newConsistentPoint constructs an empty consistentPoint for the
// specified flavor.
func newConsistentPoint(flavor string) *consistentPoint {
	switch flavor {
	case mysql.MariaDBFlavor:
		return &consistentPoint{
			ma: &mysql.MariadbGTIDSet{
				Sets: make(map[uint32]map[uint32]*mysql.MariadbGTID),
			},
		}

	case mysql.MySQLFlavor:
		return &consistentPoint{
			my: &mysql.MysqlGTIDSet{
				Sets: make(map[string]*mysql.UUIDSet),
			},
		}

	default:
		panic(errors.Errorf("Invalid flavor %s", flavor))
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

// AsTime implements logical.TimeStamp.
func (c *consistentPoint) AsTime() time.Time {
	return c.ts
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

// String is for debugging use only.
func (c *consistentPoint) String() string {
	return c.AsGTIDSet().String()
}

func (c *consistentPoint) clone() *consistentPoint {
	switch {
	case c.ma != nil:
		cloned := c.ma.Clone().(*mysql.MariadbGTIDSet)
		return &consistentPoint{ma: cloned, ts: c.ts}
	case c.my != nil:
		cloned := c.my.Clone().(*mysql.MysqlGTIDSet)
		return &consistentPoint{my: cloned, ts: c.ts}
	default:
		return &consistentPoint{ts: c.ts}
	}
}

// parseFrom decodes GTID Sets expressed as strings. This method returns
// the receiver.
//
// Supports MySQL or MariaDB
// See https://dev.mysql.com/doc/refman/8.0/en/replication-gtids-concepts.html
// and https://mariadb.com/kb/en/gtid/
// Examples:
// MySQL: E11FA47-71CA-11E1-9E33-C80AA9429562:1-3:11:47-49
// MariaDB: 0-1-1
func (c *consistentPoint) parseFrom(text string) (*consistentPoint, error) {
	switch {
	case c.ma != nil:
		set, err := mysql.ParseMariadbGTIDSet(text)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		c.ma = set.(*mysql.MariadbGTIDSet)

	case c.my != nil:
		set, err := mysql.ParseMysqlGTIDSet(text)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		c.my = set.(*mysql.MysqlGTIDSet)

	default:
		return nil, errors.New("no flavor configured")
	}
	return c, nil
}

func (c *consistentPoint) withMariaGTIDSet(
	ts time.Time, set *mysql.MariadbGTID,
) (*consistentPoint, error) {
	cloned := c.ma.Clone().(*mysql.MariadbGTIDSet)
	if err := cloned.AddSet(set); err != nil {
		return c, err
	}
	return &consistentPoint{ma: cloned, ts: ts}, nil
}

func (c *consistentPoint) withMysqlGTIDSet(ts time.Time, set *mysql.UUIDSet) *consistentPoint {
	cloned := c.my.Clone().(*mysql.MysqlGTIDSet)
	cloned.AddSet(set)
	return &consistentPoint{my: cloned, ts: ts}
}

type consistentPointPayload struct {
	Flavor string    `json:"flavor"`
	GTID   string    `json:"gtid"`
	TS     time.Time `json:"ts"`
}

func (c *consistentPoint) MarshalJSON() ([]byte, error) {
	p := consistentPointPayload{TS: c.ts}
	switch {
	case c.ma != nil:
		p.Flavor = mysql.MariaDBFlavor
		p.GTID = c.ma.String()
	case c.my != nil:
		p.Flavor = mysql.MySQLFlavor
		p.GTID = c.my.String()
	default:
		return nil, errors.New("consistentPoint not initialized")
	}
	return json.Marshal(p)
}

func (c *consistentPoint) UnmarshalJSON(data []byte) error {
	var p consistentPointPayload
	if err := json.Unmarshal(data, &p); err != nil {
		return errors.WithStack(err)
	}
	c.ts = p.TS
	_, err := c.parseFrom(p.GTID)
	return err
}

// UnmarshalText supports CLI flags and default values.
func (c *consistentPoint) UnmarshalText(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	_, err := c.parseFrom(string(data))
	return err
}
