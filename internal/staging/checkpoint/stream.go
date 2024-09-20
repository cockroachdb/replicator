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

package checkpoint

import (
	"context"
	"encoding/json"
	"time"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const streamTemplate = "EXPERIMENTAL CHANGEFEED FOR %s WITH no_initial_scan, resolved='10s'"

// streamJob opens a core changefeed over the checkpoints table to
// provide a cross-instance notification channel.
func (r *Group) streamJob(ctx *stopper.Context) {
	// This is a defensive check; this should almost always be the case.
	var enabled bool
	if err := r.pool.QueryRow(ctx, "SHOW CLUSTER SETTING kv.rangefeed.enabled").Scan(&enabled); err != nil {
		log.WithError(err).Warn(
			"could not determine if rangefeeds are enabled; polling checkpoints table")
		return
	}
	if !enabled {
		log.Warn("rangefeeds not enabled on staging cluster; polling checkpoints table")
		return
	}
	ctx.Go(func(ctx *stopper.Context) error {
		for !ctx.IsStopping() {
			if err := r.doStream(ctx); err != nil {
				if !errors.Is(err, context.Canceled) {
					log.WithError(err).Warnf("notification stream error for %s", r.target.Name)
				}
			}

			select {
			case <-ctx.Stopping():
				return nil
			case <-time.After(5 * time.Second):
				// Just restart
			}
		}
		return nil
	})
}

func (r *Group) doStream(ctx *stopper.Context) error {
	const timeout = time.Minute

	// There's no underlying heartbeat mechanism on the wire to know
	// that the stream hasn't disappeared on us.
	watchdog := time.NewTicker(timeout)
	defer watchdog.Stop()

	dbCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	ctx.Go(func(ctx *stopper.Context) error {
		select {
		case <-ctx.Stopping():
			cancel(stopper.ErrStopped)
		case <-dbCtx.Done():
			// Already canceled.
		case <-watchdog.C:
			cancel(errors.New("cdc watchdog timer"))
		}
		return nil
	})

	rows, err := r.pool.Query(dbCtx, r.sql.stream)
	if err != nil {
		return errors.Wrap(err, r.sql.stream)
	}
	defer rows.Close()

	for rows.Next() {
		watchdog.Reset(timeout)

		// We'll see a NULL value for resolved-timestamp notifications.
		var maybeTable *string
		var key, payload json.RawMessage
		if err := rows.Scan(&maybeTable, &key, &payload); err != nil {
			return errors.WithStack(err)
		}

		// Resolved timestamp to reset the watchdog.
		if maybeTable == nil {
			continue
		}

		var envelope struct {
			After json.RawMessage `json:"after"`
		}

		if err := json.Unmarshal(payload, &envelope); err != nil {
			return errors.WithStack(err)
		}

		// No-op update. We didn't request resolved timestamps.
		if len(envelope.After) == 0 {
			continue
		}

		// Keep in sync with schema.
		var tableRow struct {
			Group           ident.Ident `json:"group_name"`
			TargetAppliedAt string      `json:"target_applied_at"`
		}

		if err := json.Unmarshal(envelope.After, &tableRow); err != nil {
			return errors.WithStack(err)
		}

		// Ignore updates for other groups.
		if !ident.Equal(tableRow.Group, r.target.Name) {
			continue
		}

		// We're likely to see multiple updates in a short period of
		// time, but the refresh job loop debounces.
		log.Tracef("changefeed wakeup for %s", r.target.Name)
		r.fastWakeup.Notify()
	}
	return errors.WithStack(rows.Err())
}
