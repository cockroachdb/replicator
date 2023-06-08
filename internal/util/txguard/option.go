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

package txguard

import "time"

// Option can be passed to New.
type Option interface {
	apply(g *Guard)
}

type withMaxMisses int

func (o withMaxMisses) apply(g *Guard) {
	g.maxMisses = int(o)
}

// WithMaxMisses returns an Option that controls how many guard periods
// can elapse before the transaction is rolled back.
func WithMaxMisses(count int) Option {
	return withMaxMisses(count)
}

type withPeriod time.Duration

func (o withPeriod) apply(g *Guard) {
	g.period = time.Duration(o)
}

// WithPeriod controls how often the keepalive statement is executed
// and how often the Guard checks for calls to Guard.IsAlive.
func WithPeriod(d time.Duration) Option {
	return withPeriod(d)
}

type withQuery string

func (o withQuery) apply(g *Guard) {
	g.query = string(o)
}

// WithQuery allows the health-check database query to be overridden.
func WithQuery(s string) Option {
	return withQuery(s)
}
