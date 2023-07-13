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

// Package version is used to track incompatible schema changes within
// the staging tables. This package does not assume that breaking
// changes are necessarily linear.
//
// In the future, new versions  could trigger a lease acquisition and an
// automated migration. For the moment, we'll print a list of PR numbers
// that will contain information about the breaking change.
//
// To introduce a new version, add an entry to the tail of the
// [Versions] array, create a PR, and then update the entry with the new
// PR number.
package version

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/pkg/errors"
)

// Versions contains breaking changes to the cdc-sink metadata tables.
var Versions = []Version{
	{"Add versions table", 400},
}

// A Version describes a breaking change in the cdc-sink metadata
// tables.
type Version struct {
	Info string // A short string for human consumption.
	PR   int    // The pull request number which introduces the incompatible change.
}

// String is for debugging use only. Use [Version.Warning] for a
// user-friendly message.
func (v *Version) String() string {
	return fmt.Sprintf("%d: %s", v.PR, v.Info)
}

// Warning returns a user-facing warning message to describe the Version.
func (v *Version) Warning() string {
	return fmt.Sprintf(warningTemplate, v.Info, v.PR)
}

// In the future, versions
const (
	appliedState    = "applied"
	versionKey      = "version-%d"
	warningTemplate = `Manual schema change required to upgrade to this version of cdc-sink.
%s
See https://github.com/cockroachdb/cdc-sink/pull/%d for additional details.`
)

// Checker ensures that a set of version entries exists in the memo
// table so that we can provide useful messages to users on how to
// perform cdc-sink metadata schema fixups.
type Checker struct {
	Memo        types.Memo
	StagingPool *types.StagingPool
}

// Check will produce a report of PRs that have not been applied to the
func (c *Checker) Check(ctx context.Context) ([]string, error) {
	type payload struct {
		State string `json:"state,omitempty"`
	}

	var warnings []string
	err := retry.Retry(ctx, func(ctx context.Context) error {
		warnings = nil

		tx, err := c.StagingPool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		bootstrap := false
		for idx, v := range Versions {
			var p payload
			key := fmt.Sprintf(versionKey, v.PR)
			data, err := c.Memo.Get(ctx, tx, key)
			if err != nil {
				return errors.Wrapf(err, "could not retrieve %s", key)
			}

			// If we have data, make sure it's marked as being applied.
			if len(data) != 0 {
				if err := json.Unmarshal(data, &p); err != nil {
					return errors.Wrapf(err, "could not decode version-check payload %s", key)
				}
				// Future-proof against automatic migrations.
				if p.State != appliedState {
					warnings = append(warnings,
						fmt.Sprintf("unexpected state %s: %s", p.State, v.Warning()))
				}
				continue
			}

			// If there's no marker for the first version, we'll assume
			// that we're booting for the first time and apply markers.
			if idx == 0 || bootstrap {
				bootstrap = true
				p = payload{State: appliedState}
				data, err := json.Marshal(p)
				if err != nil {
					return errors.Wrapf(err, "could not marshal payload for %s", key)
				}
				if err := c.Memo.Put(ctx, tx, key, data); err != nil {
					return errors.Wrapf(err, "could not put %s", key)
				}
				continue
			}

			warnings = append(warnings, v.Warning())
		}

		return tx.Commit(ctx)
	})
	return warnings, err
}
