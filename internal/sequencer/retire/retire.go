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

// Package retire contains a utility for removing old mutations.
package retire

import (
	"time"

	"github.com/cockroachdb/cdc-sink/internal/sequencer"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/notify"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/cockroachdb/cdc-sink/internal/util/stopvar"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Retire implements a utility process for removing old mutations.
type Retire struct {
	cfg     *sequencer.Config
	pool    *types.StagingPool
	stagers types.Stagers
}

// Start a goroutine to ensure that old mutations are eventually
// discarded. Any staged mutations whose timestamp is less than the
// minimum value will be purged. This method will return a notification
// variable that emits the time before which all applied, staged
// mutations will have been purged.
func (r *Retire) Start(
	ctx *stopper.Context, group *types.TableGroup, bounds *notify.Var[hlc.Range],
) *notify.Var[hlc.Time] {
	ret := &notify.Var[hlc.Time]{}
	ctx.Go(func() error {
		for {
			_, err := stopvar.DoWhenChanged(ctx, hlc.Range{}, bounds, func(ctx *stopper.Context, _, bounds hlc.Range) error {
				before := bounds.Min()
				before = hlc.New(before.Nanos()-r.cfg.RetireOffset.Nanoseconds(), before.Logical())
				if hlc.Compare(before, hlc.Zero()) <= 0 {
					return nil
				}

				log.Tracef("retiring mutations in %s <= %s (%s offset)", group, before, r.cfg.RetireOffset)
				for _, tbl := range group.Tables {
					stager, err := r.stagers.Get(ctx, tbl)
					if err != nil {
						return errors.Wrapf(err, "could not acquire stager")
					}
					if err := stager.Retire(ctx, r.pool, before); err != nil {
						return errors.Wrapf(err, "could not retire mutations in %s", tbl.Raw())
					}
				}
				// Notify listeners of success.
				ret.Set(before)
				log.Tracef("retired mutations in %s <= %s (%s offset)", group, before, r.cfg.RetireOffset)
				return nil
			})
			if err != nil {
				log.WithError(err).Warn("error when trying to purge old mutations; will continue")
			}
			select {
			case <-ctx.Stopping():
				return nil
			case <-time.After(time.Second):
				// Delay to prevent log spam.
			}
		}
	})
	return ret
}
