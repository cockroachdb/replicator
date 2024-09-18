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

package oraclelogminer

import (
	"strconv"
	"time"

	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/pkg/errors"
)

func scnToHLCTime(scnStr string) (hlc.Time, error) {
	baseTime := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	scn, err := strconv.ParseInt(scnStr, 10, 64)
	if err != nil {
		return hlc.Time{}, errors.Wrapf(err, "failed to parse the scn string %s into integer", scnStr)
	}

	// Calculate timestamp by adding (SCN * 1ms) to baseTime
	// Here, we use 1 millisecond per SCN increment
	return hlc.From(baseTime.Add(time.Duration(scn) * time.Millisecond)), nil
}
