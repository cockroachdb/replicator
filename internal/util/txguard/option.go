// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
