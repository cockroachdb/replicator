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

package notify

import (
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestVar(t *testing.T) {
	r := require.New(t)

	var v Var[int]

	// Test zero value.
	current, ch := v.Get()
	r.Zero(current)
	r.NotNil(ch)
	select {
	case <-ch:
		r.Fail("channel should be open")
	default:
	}

	// Set should close the channel.
	v.Set(1)
	select {
	case <-ch:
	// OK
	case <-time.After(time.Second):
		r.Fail("channel should be closed")
	}

	// Verify we see the new value.
	current, ch2 := v.Get()
	r.Equal(1, current)
	r.NotSame(ch, ch2)

	// Read-locked callback.
	unchangedCh, err := v.Peek(func(value int) error {
		r.Equal(current, value)
		return nil
	})
	r.Equal(ch2, unchangedCh)
	r.NoError(err)

	// Check the atomic update function.
	next, _, err := v.Update(func(old int) (int, error) {
		r.Equal(1, old)
		return 2, nil
	})
	r.Equal(2, next)
	r.NoError(err)

	select {
	case <-ch:
	// OK
	case <-time.After(time.Second):
		r.Fail("channel should be closed")
	}

	// Check result of update.
	current, ch3 := v.Get()
	r.Equal(2, current)

	// Verify unchanged if Update fails.
	current, chNoUpdate, err := v.Update(func(old int) (int, error) {
		r.Equal(2, old)
		return -1, errors.New("expected")
	})
	r.Equal(2, current)
	r.ErrorContains(err, "expected")
	r.Equal(ch3, chNoUpdate)
	select {
	case <-ch3:
		r.Fail("no update, so channel should not have closed")
	default:
	}

	// Just invalidate the channel.
	v.Notify()
	select {
	case <-ch3:
	case <-time.After(time.Second):
		r.Fail("channel should be closed")
	}
}
