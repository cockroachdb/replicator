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
	restoreSchemaBackup(ctx *stopper.Context, schema ident.Schema) (*types.SchemaData, error)
	maintainSchemaBackups(ctx *stopper.Context, schemaVar *notify.Var[*types.SchemaData], schema ident.Schema)
}

type memoBackup struct {
	memo        types.Memo
	stagingPool *types.StagingPool
}

func (b *memoBackup) memoKey(schema ident.Schema) string {
	return fmt.Sprintf("schema-%v", schema)
}

func (b *memoBackup) restoreSchemaBackup(ctx *stopper.Context, schema ident.Schema) (*types.SchemaData, error) {
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

func (b *memoBackup) maintainSchemaBackups(ctx *stopper.Context, schemaVar *notify.Var[*types.SchemaData], schema ident.Schema) {
	// watch schemaVar and write changes as a staging memo
	ctx.Go(func(ctx *stopper.Context) error {
		oldValue, ch := schemaVar.Get()
		for {
			select {
			case <-ctx.Stopping():
				return nil
			case <-ch:
				var d *types.SchemaData
				d, ch = schemaVar.Get()
				// TODO: Is change detection a good idea here, or is that handled upstream?
				if !reflect.DeepEqual(d, oldValue) {
					dataBytes, err := json.Marshal(d)
					if err != nil {
						log.WithError(err).Error("failed to marshal schema data")
						return err
					}
					err = b.memo.Put(ctx, b.stagingPool, b.memoKey(schema), dataBytes)
					if err != nil {
						log.WithError(err).Warn("failed to update schema memo")
						// TODO: maybe just log this?
						return err
					}
					oldValue = d
				}
			}
		}
	})
}
