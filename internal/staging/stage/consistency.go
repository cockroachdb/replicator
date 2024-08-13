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

package stage

import (
	"context"
	"fmt"
	"os"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

var extraSanityChecks = os.Getenv("REPLICATOR_SANITY_CHECKS") != ""

// EnableSanityChecks can be called by test code to enable additional
// validation of the staging tables.
func EnableSanityChecks() {
	extraSanityChecks = true
}

const checkTemplate = `
WITH o AS (
  SELECT nanos, logical, key,
         row_number() OVER (PARTITION BY key ORDER BY nanos, logical) source_order,
         row_number() OVER (PARTITION BY key ORDER BY applied_at NULLS LAST, nanos, logical) apply_order
    FROM %[1]s
    %[2]s
)
SELECT key, count(*)
  FROM o %[3]s
 WHERE source_order != apply_order
 GROUP BY key
`

// CheckConsistency implements [types.Stager].
func (s *stage) CheckConsistency(
	ctx context.Context, db types.StagingQuerier, muts []types.Mutation, followerRead bool,
) (int, error) {
	var aost, in string
	var args []any
	if followerRead {
		aost = "AS OF SYSTEM TIME follower_read_timestamp()"
	}
	if len(muts) > 0 {
		in = "WHERE key IN (SELECT unnest($1::STRING[]))"

		keys := make([]string, len(muts))
		for idx, mut := range muts {
			keys[idx] = string(mut.Key)
		}
		args = []any{keys}
	}
	q := fmt.Sprintf(checkTemplate, s.stage.Base, in, aost)
	rows, err := db.Query(ctx, q, args...)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	var count int
	trace := log.IsLevelEnabled(log.TraceLevel)

	for rows.Next() {
		var key string
		var keyCount int
		if err := rows.Scan(&key, &keyCount); err != nil {
			return 0, errors.WithStack(err)
		}
		count += keyCount
		if trace {
			log.WithFields(log.Fields{
				"count": keyCount,
				"table": s.stage.Base,
				"key":   key,
			}).Trace("consistency check failed")
		}
	}
	if err := rows.Err(); err != nil {
		return 0, errors.WithStack(err)
	}

	return count, nil
}
