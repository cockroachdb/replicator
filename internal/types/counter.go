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

package types

import "context"

// Counter tracks the number of times an operation is executed.
type Counter interface {
	Add(float64)
}

// A countingAcceptor records top-of-funnel mutations counts and further
// classifies them based on success or failure.
type countingAcceptor struct {
	delegate  MultiAcceptor
	errors    Counter
	received  Counter
	successes Counter
}

// CountingAcceptor instantiates a new acceptor that records top-of-funnel mutations counts,
// classifies them based on success or failure, and then delegates to another acceptor.
func CountingAcceptor(
	delegate MultiAcceptor, errors Counter, received Counter, successes Counter,
) MultiAcceptor {
	if delegate == nil || errors == nil || received == nil || successes == nil {
		panic("counting acceptor delegate and counters must be provided")
	}
	return &countingAcceptor{
		delegate:  delegate,
		errors:    errors,
		received:  received,
		successes: successes,
	}
}

var _ MultiAcceptor = (*countingAcceptor)(nil)

// AcceptMultiBatch implements MultiAcceptor
func (c *countingAcceptor) AcceptMultiBatch(
	ctx context.Context, batch *MultiBatch, opts *AcceptOptions,
) error {
	count := float64(batch.Count())
	c.received.Add(count)
	err := c.delegate.AcceptMultiBatch(ctx, batch, opts)
	if err == nil {
		c.successes.Add(count)
	} else {
		c.errors.Add(count)
	}
	return err
}

// AcceptTableBatch implements MultiAcceptor
func (c *countingAcceptor) AcceptTableBatch(
	ctx context.Context, batch *TableBatch, opts *AcceptOptions,
) error {
	count := float64(batch.Count())
	c.received.Add(count)
	err := c.delegate.AcceptTableBatch(ctx, batch, opts)
	if err == nil {
		c.successes.Add(count)
	} else {
		c.errors.Add(count)
	}
	return err
}

// AcceptTemporalBatch implements MultiAcceptor
func (c *countingAcceptor) AcceptTemporalBatch(
	ctx context.Context, batch *TemporalBatch, opts *AcceptOptions,
) error {
	count := float64(batch.Count())
	c.received.Add(count)
	err := c.delegate.AcceptTemporalBatch(ctx, batch, opts)
	if err == nil {
		c.successes.Add(count)
	} else {
		c.errors.Add(count)
	}
	return err
}
