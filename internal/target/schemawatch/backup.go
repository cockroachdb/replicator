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

package schemawatch

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/ident"
	log "github.com/sirupsen/logrus"
)

type Backup interface {
	backup(ctx *stopper.Context, schema ident.Schema, data *types.SchemaData) error
	restore(ctx *stopper.Context, schema ident.Schema) (*types.SchemaData, error)
	startUpdates(ctx *stopper.Context, schemaVar *notify.Var[*types.SchemaData], schema ident.Schema) <-chan struct{}
}

type memoBackup struct {
	memo        types.Memo
	stagingPool *types.StagingPool
}

func (b *memoBackup) backup(ctx *stopper.Context, schema ident.Schema, data *types.SchemaData) error {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshalling schema data: %w", err)
	}
	err = b.memo.Put(ctx, b.stagingPool, b.memoKey(schema), dataBytes)
	if err != nil {
		return fmt.Errorf("saving schema data: %w", err)
	}
	return nil
}

func (b *memoBackup) memoKey(schema ident.Schema) string {
	return fmt.Sprintf("schema-%v", schema)
}

func (b *memoBackup) restore(ctx *stopper.Context, schema ident.Schema) (*types.SchemaData, error) {
	schemaBytes, err := b.memo.Get(ctx, b.stagingPool, b.memoKey(schema))
	if err != nil {
		return nil, err
	}
	data := &types.SchemaData{}
	err = json.Unmarshal(schemaBytes, data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (b *memoBackup) startUpdates(ctx *stopper.Context, schemaVar *notify.Var[*types.SchemaData], schema ident.Schema) <-chan struct{} {
	started := make(chan struct{})
	// watch schemaVar and write changes as a staging memo
	ctx.Go(func(ctx *stopper.Context) error {
		oldValue, ch := schemaVar.Get()
		close(started)
		for {
			select {
			case <-ctx.Stopping():
				return nil
			case <-ch:
				var d *types.SchemaData
				d, ch = schemaVar.Get()
				if !reflect.DeepEqual(d, oldValue) {
					err := b.backup(ctx, schema, d)
					if err != nil {
						// Log the error, but don't exit the loop
						log.WithError(err).WithField("target", schema).Warn("failed to backup schema data")
					}
					oldValue = d
				}
			}
		}
	})
	return started
}
