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

// Package checkpoint contains a utility for persisting checkpoint (fka
// resolved) timestamps.
package checkpoint

import (
	"context"
	"fmt"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

// Checkpoints is a factory for [Group] instances, which manage
// checkpoint timestamps associated with a group of tables.
type Checkpoints struct {
	pool      *types.StagingPool
	metaTable ident.Table
}

// Start a background goroutine to update the provided bounds variable.
// The returned Group facade allows the bounds to be modified in
// conjunction with updating the checkpoint timestamp staging table. The
// returned Group is not memoized.
func (r *Checkpoints) Start(
	ctx *stopper.Context, group *types.TableGroup, bounds *notify.Var[hlc.Range], options ...Option,
) (*Group, error) {
	var lookahead int
	useStream := true
	for _, opt := range options {
		switch t := opt.(type) {
		case disableStream:
			useStream = false
		case limitLookahead:
			lookahead = int(t)
			if lookahead <= 0 {
				return nil, errors.New("lookahead must be greater than zero")
			}
		}
	}
	ret := r.newGroup(group, bounds, lookahead)
	// Populate data immediately.
	if err := ret.refreshBounds(ctx); err != nil {
		return nil, err
	}
	ret.refreshJob(ctx)
	ret.reportMetrics(ctx)
	if useStream {
		ret.streamJob(ctx)
	}

	return ret, nil
}

func (r *Checkpoints) newGroup(
	group *types.TableGroup, bounds *notify.Var[hlc.Range], lookahead int,
) *Group {
	ret := &Group{
		bounds: bounds,
		pool:   r.pool,
		target: group,
	}

	labels := prometheus.Labels{"schema": group.Name.Raw()}
	ret.metrics.advanceDuration = advanceDuration.With(labels)
	ret.metrics.backwards = proposedGoingBackwards.With(labels)
	ret.metrics.committedAge = committedAge.With(labels)
	ret.metrics.commitDuration = commitDuration.With(labels)
	ret.metrics.committedTime = committedTime.With(labels)
	ret.metrics.proposedAge = proposedAge.With(labels)
	ret.metrics.proposedTime = proposedTime.With(labels)
	ret.metrics.refreshDuration = refreshDuration.With(labels)

	var limit string
	if lookahead > 0 {
		// +1 to have a window that includes both the last applied
		// checkpoint and the next unapplied checkpoint.
		limit = fmt.Sprintf("WHERE r <= %d", lookahead+1)
	}
	// This query may indeed require a full table scan.
	ret.sql.refresh = fmt.Sprintf(refreshTemplate, r.metaTable, limit)
	ret.sql.stream = fmt.Sprintf(streamTemplate, r.metaTable)

	hinted := r.pool.HintNoFTS(r.metaTable)
	ret.sql.advance = fmt.Sprintf(advanceTemplate, hinted)
	ret.sql.ensure = fmt.Sprintf(ensureTemplate, hinted)
	ret.sql.commit = fmt.Sprintf(commitTemplate, hinted)
	return ret
}

const scanForTargetTemplate = `
SELECT DISTINCT group_name
FROM %[1]s
WHERE target_applied_at IS NULL
`

// ScanForTargetSchemas reports any group names that have unresolved
// timestamps.
func (r *Checkpoints) ScanForTargetSchemas(ctx context.Context) ([]ident.Schema, error) {
	rows, err := r.pool.Query(ctx, fmt.Sprintf(scanForTargetTemplate, r.metaTable))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer rows.Close()
	var ret []ident.Schema
	for rows.Next() {
		var schemaRaw string
		if err := rows.Scan(&schemaRaw); err != nil {
			return nil, errors.WithStack(err)
		}

		sch, err := ident.ParseSchema(schemaRaw)
		if err != nil {
			return nil, err
		}
		ret = append(ret, sch)
	}

	return ret, nil
}
