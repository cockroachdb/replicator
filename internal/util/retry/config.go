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

// This file was copied from
// https://github.com/cockroachdb/cockroach/blob/c6122f6e6f0d35249a5eef8cab22db49dc43a626/pkg/util/retry/retry.go

package retry

import (
	"math"
	"time"

	"github.com/pkg/errors"
)

// Settings stands for the settings for a retry attempt.
type Settings struct {
	InitialBackoff time.Duration
	Multiplier     int
	MaxBackoff     time.Duration
	MaxRetries     int
}

// Verify is to confirm if the retry settings are solid.
func (s Settings) Verify() error {
	if s.InitialBackoff <= 0 {
		return errors.Errorf("initial backoff must be set to >= 0, got %s", s.InitialBackoff)
	}
	if s.Multiplier < 1 {
		return errors.Errorf("multiplier must be >= 1, got %d", s.Multiplier)
	}
	if s.MaxBackoff > 0 && s.InitialBackoff > s.MaxBackoff {
		return errors.Errorf("initial backoff (%s) must be less than max backoff (%s)", s.InitialBackoff, s.MaxBackoff)
	}
	return nil
}

// DefaultSettings returns the default settings for a retry attempt.
func DefaultSettings() Settings {
	return Settings{
		InitialBackoff: time.Second,
		Multiplier:     2,
	}
}

// Attempt is used to construct a retry operation.
type Attempt struct {
	Iteration int
	StartTime time.Time
	NextRetry time.Time

	settings Settings
}

// NewRetry returns a new Attempt variable, with given settings and now() as the start time.
func NewRetry(settings Settings) (*Attempt, error) {
	return NewRetryWithTime(time.Now(), settings)
}

// MustRetry returns a Attempt, and panic if error happens.
func MustRetry(settings Settings) *Attempt {
	r, err := NewRetryWithTime(time.Now(), settings)
	if err != nil {
		panic(err)
	}
	return r
}

// NewRetryWithTime is similar to NewRetry but set the start time with given timestamp.
func NewRetryWithTime(t time.Time, settings Settings) (*Attempt, error) {
	if err := settings.Verify(); err != nil {
		return nil, err
	}
	return &Attempt{
		Iteration: 1,
		StartTime: t,
		NextRetry: t.Add(settings.InitialBackoff),
		settings:  settings,
	}, nil
}

// MustRetryWithTime is similar to NewRetryWithTime but just panic if error happens.
func MustRetryWithTime(t time.Time, settings Settings) *Attempt {
	r, err := NewRetryWithTime(t, settings)
	if err != nil {
		panic(err)
	}
	return r
}

// ShouldContinue check if the retry should continue.
func (rm *Attempt) ShouldContinue() bool {
	if rm.settings.MaxRetries == 0 {
		return true
	}
	return rm.Iteration < rm.settings.MaxRetries
}

// Next increment the retry iteration and increase the whole retry duration.
func (rm *Attempt) Next() {
	nextDuration := rm.settings.InitialBackoff * time.Duration(math.Pow(float64(rm.settings.Multiplier), float64(rm.Iteration)))
	if rm.settings.MaxBackoff > 0 && nextDuration > rm.settings.MaxBackoff {
		nextDuration = rm.settings.MaxBackoff
	}
	rm.Iteration++
	rm.NextRetry = rm.NextRetry.Add(nextDuration)
}

// Do is to execute the retry attempt.
func (rm *Attempt) Do(do func() error, onRetry func(error)) error {
	for {
		err := do()
		if err == nil {
			return nil
		}
		if !rm.ShouldContinue() {
			return err
		}
		onRetry(err)
		time.Sleep(time.Until(rm.NextRetry))
		rm.Next()
	}
}
