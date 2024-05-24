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

// Package notify contains utility code for notification behaviors.
package notify

import (
	"errors"
	"sync"
)

// ErrNoUpdate is a sentinel value that can be returned by the callback
// passed to [Var.Update].
var ErrNoUpdate = errors.New("no update required")

// A Var holds a value that can be set or retrieved. It also provides
// a channel that indicates when the value has changed.
//
// Usage notes:
//   - The zero value of Var is ready to use.
//   - Var can be called concurrently from multiple goroutines.
//   - A Var should not be copied.
//   - If the value contained by the Var is mutable, the [Var.Peek] and
//     [Var.Update] methods should be used to ensure race-free behavior.
type Var[T any] struct {
	mu struct {
		sync.RWMutex
		data    T
		updated chan struct{}
	}
}

// VarOf constructs a Var set to the initial value.
func VarOf[T any](initial T) *Var[T] {
	ret := &Var[T]{}
	ret.mu.data = initial
	ret.mu.updated = make(chan struct{})
	return ret
}

// Get returns the current (possibly zero) value for T and a channel
// that will be closed the next time that Set or Update is called. This
// API does not guarantee that a loop as shown below will see every
// update made to the Var.  Rather, it allows a consumer to sample the
// most current value available.
//
//	for value, valueUpdated := v.Get(); ;{
//	  doSomething(value)
//	  select {
//	    case <-valueUpdated:
//	      value, valueUpdated = v.Get()
//	    case <-ctx.Done():
//	      return ctx.Err()
//	  }
//	}
func (v *Var[T]) Get() (T, <-chan struct{}) {
	v.mu.RLock()
	data, ch := v.mu.data, v.mu.updated
	v.mu.RUnlock()
	if ch != nil {
		return data, ch
	}

	// Get called on zero value, may need to initialize.
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.mu.updated == nil {
		v.mu.updated = make(chan struct{})
	}
	return v.mu.data, v.mu.updated
}

// Notify behaves as though Set was called with the current value.
// That is, it replaces the notification channel.
func (v *Var[T]) Notify() {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.notifyLocked()
}

// Peek holds a read lock while the callback is invoked. This method
// will return the notification channel associated with the current
// value of the Var.
func (v *Var[T]) Peek(fn func(value T) error) (<-chan struct{}, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.mu.updated, fn(v.mu.data)
}

// Set updates the value and notifies any listeners. The notification
// channel is returned to avoid a race condition if a caller wants to
// set the value and receive a notification if another caller has
// subsequently updated it.
func (v *Var[T]) Set(next T) <-chan struct{} {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.mu.data = next
	v.notifyLocked()
	return v.mu.updated
}

// Swap returns the current value and a channel that will be closed
// when the next value has been replaced.
func (v *Var[T]) Swap(next T) (T, <-chan struct{}) {
	v.mu.Lock()
	defer v.mu.Unlock()

	ret := v.mu.data
	v.mu.data = next
	v.notifyLocked()
	return ret, v.mu.updated
}

// Update atomically updates the stored value using the current value as
// an input. The callback may return [ErrNoUpdate] to take no action;
// this error will not be returned to the caller. If the callback
// returns any other error, no action is taken and the unchanged value
// is returned.
func (v *Var[T]) Update(fn func(old T) (new T, _ error)) (T, <-chan struct{}, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	next, err := fn(v.mu.data)
	if err == nil {
		v.mu.data = next
		v.notifyLocked()
	} else if errors.Is(err, ErrNoUpdate) {
		err = nil
	}
	return v.mu.data, v.mu.updated, err
}

func (v *Var[T]) notifyLocked() {
	if ch := v.mu.updated; ch != nil {
		close(ch)
	}
	v.mu.updated = make(chan struct{})
}
