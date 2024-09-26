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

// Package scn defines SCN for oracle source.
package scn

import (
	"strconv"

	"github.com/pkg/errors"
)

// SCN stands for System Change Number to start listening. Per oracle doc, A
// system change number (SCN) is a logical, internal time stamp used
// by Oracle Database. SCNs order events that occur within the
// database, which is necessary to satisfy the ACID properties of a
// transaction. Oracle Database uses SCNs to mark the SCN before
// which all changes are known to be on disk so that recovery avoids
// applying unnecessary redo. The database also uses SCNs to mark
// the point at which no redo exists for a set of data so that
// recovery can stop. See also https://docs.oracle.com/cd/E11882_01/server.112/e40540/transact.htm#CNCPT039
type SCN struct {
	Val int
}

// Compare returns 1 if the host SCN is larger than the argument SCN,
// return -1 if the host SCN is smaller than the argument SCN,
// and 0 if both equal.
func (s *SCN) Compare(other SCN) int {
	if s.Val > other.Val {
		return 1
	} else if s.Val < other.Val {
		return -1
	}
	return 0
}

// Diff returns the difference between the host SCN and the argument SCN.
func (s *SCN) Diff(other SCN) int {
	return s.Val - other.Val
}

// IsEmpty returns true if the SCN is unset with valid value.
func (s *SCN) IsEmpty() bool {
	return s.Val == 0
}

// String returns the string representation of an SCN.
func (s SCN) String() string {
	return strconv.Itoa(s.Val)
}

// ParseStringToSCN parse a string into an SCN struct.
func ParseStringToSCN(s string) (SCN, error) {
	intRes, err := strconv.Atoi(s)
	if err != nil {
		return SCN{}, errors.Wrapf(err, "failed to parse %s into int for SCN construction", s)
	}
	return SCN{Val: intRes}, nil

}
