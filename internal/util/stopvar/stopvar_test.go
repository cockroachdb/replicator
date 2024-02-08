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

package stopvar

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/util/notify"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/stretchr/testify/require"
)

func TestDoWhenChanged(t *testing.T) {
	r := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var called atomic.Bool
	var v notify.Var[int]

	stop := stopper.WithContext(ctx)
	stop.Go(func() error {
		_, err := DoWhenChanged(stop, -1, &v, func(ctx *stopper.Context, old, new int) error {
			switch new {
			case 1:
				r.Equal(-1, old)
				v.Set(2) // This should cause us to loop around.
			case 2:
				r.Equal(1, old)
				called.Store(true)
				stop.Stop(time.Minute)
			}
			return nil
		})
		return err
	})

	v.Set(1)
	r.NoError(stop.Wait())
	r.True(called.Load())
}

func TestDoWhenChangedOrInterval(t *testing.T) {
	r := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var called atomic.Bool
	sawMinusOne := make(chan struct{})
	var v notify.Var[int]

	stop := stopper.WithContext(ctx)
	stop.Go(func() error {
		_, err := DoWhenChangedOrInterval(stop, -1, &v, time.Millisecond,
			func(ctx *stopper.Context, old, new int) error {
				// The expected sequence old (old, new):
				// (-1, 0)
				// (0, 0) ...
				// (0, 1)
				// (1, 1) ...
				// (1, 2)
				// (2, 2) ...

				switch {
				case old == new:
					// Looping due to timer.
				case old == -1 && new == 0:
					close(sawMinusOne)
				case old == 0 && new == 1:
					v.Set(2)
				case old == 1 && new == 2:
					called.Store(true)
					stop.Stop(time.Minute)
				default:
					r.Failf("unexpected state", "old=%d, new=%d", old, new)
				}

				return nil
			})
		return err
	})

	<-sawMinusOne
	v.Set(1)
	r.NoError(stop.Wait())
	r.True(called.Load())
}

func TestWaitForValue(t *testing.T) {
	r := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var called atomic.Bool
	var v notify.Var[int]

	stop := stopper.WithContext(ctx)
	stop.Go(func() error {
		if err := WaitForValue(stop, 1, &v); err != nil {
			return err
		}
		called.Store(true)
		stop.Stop(time.Minute)
		return nil
	})

	v.Set(1)
	r.NoError(stop.Wait())
	r.True(called.Load())

}
