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

	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/metrics"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	dto "github.com/prometheus/client_model/go"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	mutationsErrorCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mutations_error_count",
		Help: "the total number of mutations that encountered an error during processing",
	}, []string{"target"})
	mutationsReceivedCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mutations_received_count",
		Help: "the total number of mutations received from the source",
	}, []string{"target"})
	mutationsSuccessCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mutations_success_count",
		Help: "the total number of mutations that were successfully processed",
	}, []string{"target"})
	testCounterSchema = ident.MustSchema(ident.New("my_db"), ident.New("public"))
	testCounterTable  = ident.NewTable(testCounterSchema, ident.New("my_table"))
)

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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mutationsErrorCount.Reset()
	mutationsReceivedCount.Reset()
	mutationsSuccessCount.Reset()
	delegate := &boundedAcceptor{max: 10}
	for i := 0; i < 5; i++ {
		r.NoError(batch.Accumulate(testCounterTable, Mutation{
			Time: hlc.New(1, 0),
		}))
	}
	labels := metrics.SchemaValues(testCounterSchema)
	countingAcceptor := CountingAcceptor(delegate,
		mutationsErrorCount.WithLabelValues(labels...),
		mutationsReceivedCount.WithLabelValues(labels...),
		mutationsSuccessCount.WithLabelValues(labels...))

	var err error
	switch b := any(batch).(type) {
	case *MultiBatch:
		err = countingAcceptor.AcceptMultiBatch(ctx, b, &AcceptOptions{})
	case *TableBatch:
		err = countingAcceptor.AcceptTableBatch(ctx, b, &AcceptOptions{})
	case *TemporalBatch:
		err = countingAcceptor.AcceptTemporalBatch(ctx, b, &AcceptOptions{})
	default:
		a.Fail(fmt.Sprintf("batch  %T not supported", b))
	}

	a.NoError(err)
	a.Equal(0, getCounterValue(t, mutationsErrorCount))
	a.Equal(5, getCounterValue(t, mutationsReceivedCount))
	a.Equal(5, getCounterValue(t, mutationsSuccessCount))

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
		a.Fail(fmt.Sprintf("batch  %T not supported", b))
	}
	a.Error(err)
	a.Equal(5, getCounterValue(t, mutationsErrorCount))
	a.Equal(10, getCounterValue(t, mutationsReceivedCount))
	a.Equal(5, getCounterValue(t, mutationsSuccessCount))
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
	log.Infof("current %d", b.current)
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
	log.Infof("current %d", b.current)
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
	log.Infof("current %d", b.current)
	return nil
}

// getCounterValue extracts the current value of a counter for the specified schema.
func getCounterValue(t *testing.T, counter *prometheus.CounterVec) int {
	r := require.New(t)
	var metric = &dto.Metric{}
	labels := metrics.SchemaValues(testCounterSchema)
	err := counter.WithLabelValues(labels...).Write(metric)
	r.NoError(err)
	return int(metric.Counter.GetValue())
}
