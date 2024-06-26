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

package objstore

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/pkg/errors"
)

// state is used to persist the last entry we processed from the object storage.
type state struct {
	key  string
	memo types.Memo
}

// valuePayload represent the last entry processed from the object storage.
type valuePayload struct {
	Position string `json:"position"`
}

// getLast retrieves the last entry processed from the object storage.
func (s *state) getLast(ctx context.Context, tx types.StagingQuerier) (string, error) {
	v, err := s.memo.Get(ctx, tx, s.key)
	if err != nil {
		return "", err
	}
	value := valuePayload{}
	if v != nil {
		dec := json.NewDecoder(bytes.NewReader(v))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&value); err != nil {
			return "", errors.WithStack(err)
		}
	}
	return value.Position, nil
}

// setLast stores the last entry processed from the object storage.
func (s *state) setLast(ctx context.Context, tx types.StagingQuerier, last string) error {
	value, err := json.Marshal(valuePayload{Position: last})
	if err != nil {
		return err
	}
	return s.memo.Put(ctx, tx, s.key, value)
}
