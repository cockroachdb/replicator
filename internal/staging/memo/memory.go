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

package memo

import (
	"context"
	"sync"

	"github.com/cockroachdb/replicator/internal/types"
)

// Memory is an implementation of types.Memo backed by memory, used for
// testing. Transactions are ignored.
type Memory struct {
	values sync.Map
}

var _ types.Memo = &Memory{}

// Get implements types.Memo.
func (m *Memory) Get(_ context.Context, _ types.StagingQuerier, key string) ([]byte, error) {
	res, ok := m.values.Load(key)
	if !ok {
		return nil, nil
	}
	return res.([]byte), nil
}

// Put implements types.Memo.
func (m *Memory) Put(_ context.Context, _ types.StagingQuerier, key string, value []byte) error {
	m.values.Store(key, value)
	return nil
}
