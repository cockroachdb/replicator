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

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testCounterSchema = ident.MustSchema(ident.New("my_db"), ident.New("public"))
	testCounterTable  = ident.NewTable(testCounterSchema, ident.New("my_table"))
)

type counter float64

func (c *counter) Add(f float64) {
	*c = counter(float64(*c) + f)
}

func (c *counter) IntValue() int {
	return int(*c)
}

// TestCountingAcceptor verifies that the CountingAcceptor correctly
// tracks mutations.
func TestCountingAcceptor(t *testing.T) {
	testCountingAcceptor(t, &MultiBatch{})
	testCountingAcceptor(t, &TableBatch{
		Time:  hlc.New(1, 0),
		Table: testCounterTable,
	})
	testCountingAcceptor(t, &TemporalBatch{
		Time: hlc.New(1, 0),
	})
}

func testCountingAcceptor[B Batch[B]](t *testing.T, batch B) {
	r := require.New(t)
	a := assert.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	delegate := &boundedAcceptor{max: 10}
	for i := 0; i < 5; i++ {
		r.NoError(batch.Accumulate(testCounterTable, Mutation{
			Time: hlc.New(1, 0),
		}))
	}
	errors := counter(0)
	received := counter(0)
	successes := counter(0)
	countingAcceptor := CountingAcceptor(delegate, &errors, &received, &successes)

	var err error
	switch b := any(batch).(type) {
	case *MultiBatch:
		err = countingAcceptor.AcceptMultiBatch(ctx, b, &AcceptOptions{})
	case *TableBatch:
		err = countingAcceptor.AcceptTableBatch(ctx, b, &AcceptOptions{})
	case *TemporalBatch:
		err = countingAcceptor.AcceptTemporalBatch(ctx, b, &AcceptOptions{})
	default:
		a.Fail(fmt.Sprintf("batch %T not supported", b))
	}

	a.NoError(err)
	a.Equal(0, errors.IntValue())
	a.Equal(5, received.IntValue())
	a.Equal(5, successes.IntValue())

	batch = batch.Empty()
	for i := 0; i < 5; i++ {
		r.NoError(batch.Accumulate(testCounterTable, Mutation{
			Time: hlc.New(1, 0),
		}))
	}
	switch b := any(batch).(type) {
	case *MultiBatch:
		err = countingAcceptor.AcceptMultiBatch(ctx, b, &AcceptOptions{})
	case *TableBatch:
		err = countingAcceptor.AcceptTableBatch(ctx, b, &AcceptOptions{})
	case *TemporalBatch:
		err = countingAcceptor.AcceptTemporalBatch(ctx, b, &AcceptOptions{})
	default:
		a.Fail(fmt.Sprintf("batch %T not supported", b))
	}
	a.Error(err)
	a.Equal(5, errors.IntValue())
	a.Equal(10, received.IntValue())
	a.Equal(5, successes.IntValue())
}

type boundedAcceptor struct {
	max     int
	current int
}

// AcceptMultiBatch implements MultiAcceptor.
func (b *boundedAcceptor) AcceptMultiBatch(
	_ context.Context, batch *MultiBatch, _ *AcceptOptions,
) error {
	if b.current+batch.Count() >= b.max {
		return errors.New("too many elements")
	}
	b.current = b.current + batch.Count()
	return nil
}

// AcceptTableBatch implements MultiAcceptor.
func (b *boundedAcceptor) AcceptTableBatch(
	_ context.Context, batch *TableBatch, _ *AcceptOptions,
) error {
	if b.current+batch.Count() >= b.max {
		return errors.New("too many elements")
	}
	b.current = b.current + batch.Count()
	return nil
}

// AcceptTemporalBatch implements MultiAcceptor.
func (b *boundedAcceptor) AcceptTemporalBatch(
	_ context.Context, batch *TemporalBatch, _ *AcceptOptions,
) error {
	if b.current+batch.Count() >= b.max {
		return errors.New("too many elements")
	}
	b.current = b.current + batch.Count()
	return nil
}
