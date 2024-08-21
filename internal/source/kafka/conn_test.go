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

package kafka

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockConveyor struct {
	mu struct {
		sync.Mutex
		done       bool
		ensure     ident.Map[bool]
		timestamps ident.Map[hlc.Time]
	}
}

var _ Conveyor = &mockConveyor{}
var sentinel = hlc.New(9999, 0)

// AcceptMultiBatch implements Target.
func (a *mockConveyor) AcceptMultiBatch(
	_ context.Context, batch *types.MultiBatch, _ *types.AcceptOptions,
) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if batch.ByTime[sentinel] != nil {
		a.mu.done = true
		log.Info("AcceptMultiBatch found sentinel")
	}
	return nil
}

// Advance implements Target.
func (a *mockConveyor) Advance(_ context.Context, partition ident.Ident, ts hlc.Time) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.mu.timestamps.Put(partition, ts)
	return nil
}

// Ensure implements Target.
func (a *mockConveyor) Ensure(_ context.Context, partitions []ident.Ident) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, p := range partitions {
		a.mu.ensure.Put(p, true)
	}
	return nil
}

// Watcher implements Target. Not used in this test.
func (a *mockConveyor) Watcher() types.Watcher {
	return nil
}

func (a *mockConveyor) done() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.mu.done
}

func (a *mockConveyor) getEnsured(partition ident.Ident) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.mu.ensure.GetZero(partition)
}
func (a *mockConveyor) getTimestamp(partition ident.Ident) hlc.Time {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.mu.timestamps.GetZero(partition)
}

// TestConn verifies that we can process messages using a simple mock broker.
func TestConn(t *testing.T) {
	ctx := stopper.Background()
	a := assert.New(t)
	r := require.New(t)
	mb := sarama.NewMockBroker(t, 1)
	mb.SetHandlerByMap(map[string]sarama.MockResponse{
		"SaslHandshakeRequest": sarama.NewMockSaslHandshakeResponse(t).
			SetEnabledMechanisms([]string{sarama.SASLTypePlaintext}),
		"SaslAuthenticateRequest": sarama.NewMockSequence(
			sarama.NewMockSaslAuthenticateResponse(t)),
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(mb.Addr(), mb.BrokerID()).
			SetLeader("my-topic", 31, mb.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset("my-topic", 31, sarama.OffsetOldest, 0).
			SetOffset("my-topic", 31, sarama.OffsetNewest, 1),
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(t).
			SetCoordinator(sarama.CoordinatorGroup, "my-group", mb),
		"HeartbeatRequest": sarama.NewMockHeartbeatResponse(t),
		"JoinGroupRequest": sarama.NewMockSequence(
			sarama.NewMockJoinGroupResponse(t).SetError(sarama.ErrOffsetsLoadInProgress),
			sarama.NewMockJoinGroupResponse(t).SetGroupProtocol(sarama.StickyBalanceStrategyName),
		),
		"SyncGroupRequest": sarama.NewMockSequence(
			sarama.NewMockSyncGroupResponse(t).SetError(sarama.ErrOffsetsLoadInProgress),
			sarama.NewMockSyncGroupResponse(t).SetMemberAssignment(
				&sarama.ConsumerGroupMemberAssignment{
					Version: 0,
					Topics: map[string][]int32{
						"my-topic": {31},
					},
				}),
		),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(t).SetOffset(
			"my-group", "my-topic", 31, 0, "", sarama.ErrNoError,
		).SetError(sarama.ErrNoError),
		"FetchRequest": sarama.NewMockSequence(
			sarama.NewMockFetchResponse(t, 1).
				SetMessage("my-topic", 31, 0, sarama.StringEncoder(`{"resolved":"1.0"}`)),
			sarama.NewMockFetchResponse(t, 1).
				SetMessage("my-topic", 31, 1, sarama.StringEncoder(`{"after": {"k":1, "v": "a"},"updated":"2.0"}`)),
			sarama.NewMockFetchResponse(t, 1).
				SetMessage("my-topic", 31, 2, sarama.StringEncoder(`{"after": {"k":2, "v": "a"},"updated":"2.0"}`)),
			sarama.NewMockFetchResponse(t, 1).
				SetMessage("my-topic", 31, 3, sarama.StringEncoder(`{"resolved":"2.0"}`)),
			sarama.NewMockFetchResponse(t, 1).
				SetMessage("my-topic", 31, 4, sarama.StringEncoder(`{"after": {"k":2, "v": "a"},"updated":"9999.0"}`)),
		),
	})

	addr := mb.Addr()
	config := &Config{
		BatchSize:        1,
		Brokers:          []string{addr},
		Group:            "my-group",
		ResolvedInterval: time.Second,
		Strategy:         "sticky",
		Topics:           []string{"my-topic"},
		SASL: SASLConfig{
			Mechanism: sarama.SASLTypePlaintext,
			User:      "user",
			Password:  "test",
		},
	}
	err := config.preflight(ctx)
	a.NoError(err)
	conv := &mockConveyor{}

	conn := &Conn{
		config:   config,
		conveyor: conv,
	}
	connCtx := stopper.WithContext(ctx)
	err = conn.Start(connCtx)
	r.NoError(err)
	for !conv.done() {
		time.Sleep(1 * time.Second)
	}
	part := ident.New("my-topic@31")
	a.Equal(true, conv.getEnsured(part))
	a.Equal(hlc.New(2, 0), conv.getTimestamp(part))
	connCtx.Stop(time.Second)
	a.True(connCtx.IsStopping())
	mb.Close()
}
