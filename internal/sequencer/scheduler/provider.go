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

package scheduler

import (
	"context"

	"github.com/cockroachdb/cdc-sink/internal/sequencer"
	"github.com/cockroachdb/cdc-sink/internal/util/lockset"
	"github.com/cockroachdb/cdc-sink/internal/util/workgroup"
	"github.com/google/wire"
	"github.com/pkg/errors"
)

// Set is used by Wire.
var Set = wire.NewSet(ProvideScheduler)

// ProvideScheduler is called by Wire.
func ProvideScheduler(ctx context.Context, cfg *sequencer.Config) (*Scheduler, error) {
	p := cfg.Parallelism
	if p <= 0 {
		return nil, errors.Errorf("cannot schedule work on %d workers", p)
	}

	// The queue depth is sufficiently large that other things
	// should catch fire before we can't admit more work.
	locks, err := lockset.New[string](workgroup.WithSize(ctx, p, 10_000*p), "scheduler")
	if err != nil {
		return nil, err
	}

	return &Scheduler{locks}, nil
}
