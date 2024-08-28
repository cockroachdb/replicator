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

package schemawatch

import (
	"encoding/json"
	"fmt"

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/field-eng-powertools/stopvar"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Backup handles schema backups and restores. It allows staging
// progress when the target database is down.
type Backup interface {
	backup(ctx *stopper.Context, schema ident.Schema, data *types.SchemaData) error
	restore(ctx *stopper.Context, schema ident.Schema) (*types.SchemaData, error)
	startUpdates(
		ctx *stopper.Context,
		schemaVar *notify.Var[*types.SchemaData],
		schema ident.Schema,
	)
}

// memoBackup implements Backup and is backed by the staging table memo
// store
type memoBackup struct {
	memo        types.Memo
	stagingPool *types.StagingPool
}

var _ Backup = (*memoBackup)(nil)

// backup backs up a schema
func (b *memoBackup) backup(
	ctx *stopper.Context, schema ident.Schema, data *types.SchemaData,
) error {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return errors.Wrap(err, "could not marshal schema data")
	}
	err = b.memo.Put(ctx, b.stagingPool, b.memoKey(schema), dataBytes)
	if err != nil {
		return errors.Wrap(err, "couldn't save schema data")
	}
	return nil
}

// memoKey constructs a
func (b *memoBackup) memoKey(schema ident.Schema) string {
	return fmt.Sprintf("schema-%s", schema.Canonical().Raw())
}

// restore fetches the latest schema backup
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

// startUpdates watches the supplied schemaVar for changes and updates
// the backup. It records the initial value of schemaVar as the first "change".
func (b *memoBackup) startUpdates(
	ctx *stopper.Context, schemaVar *notify.Var[*types.SchemaData], schema ident.Schema,
) {
	// watch schemaVar and write changes as a staging memo
	ctx.Go(func(ctx *stopper.Context) error {
		_, err := stopvar.DoWhenChanged(ctx, nil, schemaVar, func(ctx *stopper.Context, _, d *types.SchemaData) error {
			err := b.backup(ctx, schema, d)
			if err != nil {
				// Log the error, but don't exit the loop
				log.WithError(err).WithField("target", schema).Warn("failed to backup schema data")
			}
			return nil
		})
		return err
	})
}
