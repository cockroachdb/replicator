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

package eventproc

import (
	"context"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/source/objstore/bucket"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/cdcjson"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/pkg/errors"
)

// Acceptor receives batches for processing.
type Acceptor interface {
	AcceptMultiBatch(context.Context, *types.MultiBatch, *types.AcceptOptions) error
}

// localProcessor is a Processor that runs in the same process as the client.
type localProcessor struct {
	acceptor Acceptor
	bucket   bucket.Bucket
	parser   *cdcjson.NDJsonParser
	schema   ident.Schema
}

var _ Processor = &localProcessor{}

// NewLocal creates a local processor.
func NewLocal(
	acceptor Acceptor, bucket bucket.Bucket, parser *cdcjson.NDJsonParser, schema ident.Schema,
) Processor {
	return &localProcessor{
		acceptor: acceptor,
		bucket:   bucket,
		parser:   parser,
		schema:   schema,
	}
}

// Process implements Processor
func (c *localProcessor) Process(ctx *stopper.Context, path string) error {
	// Extract the table name from the path.
	tableName, err := getTableName(path)
	if err != nil {
		return err
	}
	table := ident.NewTable(c.schema, tableName)
	// Retrieve the file content from the bucket.
	buff, err := c.bucket.Open(ctx, path)
	if err != nil {
		return errors.Wrapf(err, "failed to retrieve %s", path)
	}
	defer buff.Close()
	// Parse the mutations inside the file into a Batch.
	batch, err := c.parser.Parse(table, cdcjson.BulkMutationReader(), buff)
	if err != nil {
		return errors.Wrapf(err, "failed to parse %s", path)
	}
	// Send the batch downstream to the target.
	return c.acceptor.AcceptMultiBatch(ctx, batch, nil)
}
