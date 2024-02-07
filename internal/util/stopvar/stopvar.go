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

// Package stopvar contains helpers that build on both the stopper and
// notify packages.
package stopvar

import (
	"time"

	"github.com/cockroachdb/cdc-sink/internal/util/notify"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/pkg/errors"
)

// DoWhenChanged executes the callback in response to the variable
// changing to a distinct value. If an error is returned from the
// callback, the last successfully-processed value will be returned.
func DoWhenChanged[T comparable](
	ctx *stopper.Context,
	start T,
	source *notify.Var[T],
	fn func(ctx *stopper.Context, old, new T) error,
) (last T, err error) {
	last = start
	for {
		next, _ := WaitForChange(ctx, last, source)
		if ctx.IsStopping() {
			return last, nil
		}
		if err := fn(ctx, last, next); err != nil {
			return last, errors.Wrapf(err, "changed [%v -> %v]", last, next)
		}
		last = next
	}
}

// DoWhenChangedOrInterval executes the callback when the variable has
// change or if the configured period of time has elapsed since the last
// invocation. This is useful when some activity should be taken in
// response to change or at a somewhat regular interval. If an error is
// returned from the callback, the last successfully-processed value
// will be returned.
func DoWhenChangedOrInterval[T comparable](
	ctx *stopper.Context,
	start T,
	source *notify.Var[T],
	period time.Duration,
	fn func(ctx *stopper.Context, old, new T) error,
) (last T, err error) {
	last = start
	for {
		next, _ := WaitForChangeOrDuration(ctx, last, source, period)
		if ctx.IsStopping() {
			return last, nil
		}
		if err := fn(ctx, last, next); err != nil {
			return last, errors.Wrapf(err, "changed [%v -> %v]", last, next)
		}
		last = next
	}
}

// WaitForChange is a utility function that waits for the source to
// change to another value. If the context is stopped, the most recent
// value will be returned.
func WaitForChange[T comparable](
	ctx *stopper.Context, current T, source *notify.Var[T],
) (next T, changed <-chan struct{}) {
	for {
		next, changed = source.Get()
		if current != next {
			return next, changed
		}
		select {
		case <-changed:
			continue
		case <-ctx.Stopping():
			return current, changed
		}
	}
}

// WaitForChangeOrDuration is a utility function that waits for the
// source to change to another value or for the given duration to
// elapse.
func WaitForChangeOrDuration[T comparable](
	ctx *stopper.Context, current T, source *notify.Var[T], d time.Duration,
) (next T, changed <-chan struct{}) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		next, changed = source.Get()
		if current != next {
			return next, changed
		}
		select {
		case <-changed:
			continue
		case <-timer.C:
			return current, changed
		case <-ctx.Stopping():
			return current, changed
		}
	}
}

// WaitForValue is a utility function that waits until the source emits
// the requested value. This is primarily intended for testing.
func WaitForValue[T comparable](ctx *stopper.Context, expected T, source *notify.Var[T]) error {
	for {
		found, changed := source.Get()
		if found == expected {
			return nil
		}
		select {
		case <-changed:
			continue
		case <-ctx.Stopping():
			return errors.Errorf("context is stopping, last saw %v while expecting %v", found, expected)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
