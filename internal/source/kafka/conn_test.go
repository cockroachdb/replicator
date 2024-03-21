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
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ types.MultiAcceptor = &mockAcceptor{}

type mockAcceptor struct {
	mu struct {
		sync.Mutex
		done bool
	}
}

func (a *mockAcceptor) done() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.mu.done
}

// AcceptMultiBatch implements types.MultiAcceptor.
func (a *mockAcceptor) AcceptMultiBatch(
	_ context.Context, batch *types.MultiBatch, _ *types.AcceptOptions,
) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if batch.ByTime[hlc.New(9999, 0)] != nil {
		a.mu.done = true
		log.Info("AcceptMultiBatch found sentinel")
	}

	return nil
}

// AcceptTableBatch implements types.MultiAcceptor.
func (a *mockAcceptor) AcceptTableBatch(
	context.Context, *types.TableBatch, *types.AcceptOptions,
) error {
	return nil
}

// AcceptTemporalBatch implements types.MultiAcceptor.
func (a *mockAcceptor) AcceptTemporalBatch(
	context.Context, *types.TemporalBatch, *types.AcceptOptions,
) error {
	return nil
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
			SetLeader("my-topic", 0, mb.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset("my-topic", 0, sarama.OffsetOldest, 0).
			SetOffset("my-topic", 0, sarama.OffsetNewest, 1),
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
						"my-topic": {0},
					},
				}),
		),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(t).SetOffset(
			"my-group", "my-topic", 0, 0, "", sarama.ErrNoError,
		).SetError(sarama.ErrNoError),
		"FetchRequest": sarama.NewMockSequence(
			sarama.NewMockFetchResponse(t, 1).
				SetMessage("my-topic", 0, 0, sarama.StringEncoder(`{"resolved":"1.0"}`)),
			sarama.NewMockFetchResponse(t, 1).
				SetMessage("my-topic", 0, 1, sarama.StringEncoder(`{"after": {"k":1, "v": "a"},"updated":"2.0"}`)),
			sarama.NewMockFetchResponse(t, 1).
				SetMessage("my-topic", 0, 2, sarama.StringEncoder(`{"after": {"k":2, "v": "a"},"updated":"2.0"}`)),
			sarama.NewMockFetchResponse(t, 1).
				SetMessage("my-topic", 0, 3, sarama.StringEncoder(`{"resolved":"2.0"}`)),
			sarama.NewMockFetchResponse(t, 1).
				SetMessage("my-topic", 0, 4, sarama.StringEncoder(`{"after": {"k":2, "v": "a"},"updated":"9999.0"}`)),
		),
	})

	addr := mb.Addr()
	config := &Config{
		BatchSize:     1,
		Brokers:       []string{addr},
		Group:         "my-group",
		Strategy:      "sticky",
		Topics:        []string{"my-topic"},
		saslMechanism: sarama.SASLTypePlaintext,
		saslUser:      "user",
		saslPassword:  "test",
	}
	err := config.preflight(ctx)
	a.NoError(err)
	acceptor := &mockAcceptor{}
	conn := &Conn{
		acceptor: acceptor,
		config:   config,
	}
	err = conn.Start(ctx)
	r.NoError(err)
	for !acceptor.done() {
		time.Sleep(1 * time.Second)
	}
	mb.Close()
}
