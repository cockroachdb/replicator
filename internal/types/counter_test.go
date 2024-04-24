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

package types

import (
	"context"
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/metrics"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCountingAcceptor verifies that the CountingAcceptor correctly
// tracks mutations.
func TestCountingAcceptor(t *testing.T) {
	r := require.New(t)
	a := assert.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mutationsErrorCount.Reset()
	mutationsReceivedCount.Reset()
	mutationsSuccessCount.Reset()
	delegate := &boundedAcceptor{max: 10}
	batch := &MultiBatch{}
	schema := ident.MustSchema(ident.New("my_db"), ident.New("public"))
	table := ident.NewTable(schema, ident.New("my_table"))
	for i := 0; i < 5; i++ {
		r.NoError(batch.Accumulate(table, Mutation{
			Time: hlc.New(int64(i+1), i+1),
		}))
	}
	countingAcceptor := CountingAcceptor(delegate, schema)
	err := countingAcceptor.AcceptMultiBatch(ctx, batch, &AcceptOptions{})
	a.NoError(err)
	a.Equal(0, getCounterValue(t, schema, mutationsErrorCount))
	a.Equal(5, getCounterValue(t, schema, mutationsReceivedCount))
	a.Equal(5, getCounterValue(t, schema, mutationsSuccessCount))

	batch = batch.Empty()
	for i := 0; i < 5; i++ {
		r.NoError(batch.Accumulate(table, Mutation{
			Time: hlc.New(int64(i+1), i+1),
		}))
	}
	err = countingAcceptor.AcceptMultiBatch(ctx, batch, &AcceptOptions{})
	a.Error(err)
	a.Equal(5, getCounterValue(t, schema, mutationsErrorCount))
	a.Equal(10, getCounterValue(t, schema, mutationsReceivedCount))
	a.Equal(5, getCounterValue(t, schema, mutationsSuccessCount))
}

type boundedAcceptor struct {
	max     int
	current int
}

// AcceptMultiBatch implements MultiAcceptor.
func (b *boundedAcceptor) AcceptMultiBatch(
	_ context.Context, batch *MultiBatch, _ *AcceptOptions,
) error {
	if b.current+batch.Len() >= b.max {
		return errors.New("too many elements")
	}
	b.current = b.current + batch.Len()
	log.Infof("current %d", b.current)
	return nil
}

// AcceptTableBatch implements MultiAcceptor.
func (b *boundedAcceptor) AcceptTableBatch(context.Context, *TableBatch, *AcceptOptions) error {
	return nil
}

// AcceptTemporalBatch implements MultiAcceptor.
func (b *boundedAcceptor) AcceptTemporalBatch(
	context.Context, *TemporalBatch, *AcceptOptions,
) error {
	return nil
}

// getCounterValue extracts the current value of a counter for the specified schema.
func getCounterValue(t *testing.T, target ident.Schema, counter *prometheus.CounterVec) int {
	r := require.New(t)
	var metric = &dto.Metric{}
	labels := metrics.SchemaValues(target)
	err := counter.WithLabelValues(labels...).Write(metric)
	r.NoError(err)
	return int(metric.Counter.GetValue())
}
