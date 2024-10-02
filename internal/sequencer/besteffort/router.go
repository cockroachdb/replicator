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

package besteffort

import (
	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/ident"
	log "github.com/sirupsen/logrus"
)

type router struct {
	accept   ident.TableMap[struct{}]
	delegate types.BatchReader
}

var _ types.BatchReader = (*router)(nil)

func (f *router) Read(ctx *stopper.Context) (<-chan *types.BatchCursor, error) {
	source, err := f.delegate.Read(ctx)
	if err != nil {
		return nil, err
	}

	out := make(chan *types.BatchCursor, 2)
	ctx.Go(func(ctx *stopper.Context) error {
		defer close(out)
		for {
			var cur *types.BatchCursor
			select {
			case cur = <-source:
			case <-ctx.Stopping():
				return nil
			}
			// Stopping.
			if cur == nil {
				return nil
			}
			cur = cur.Copy()

			if cur.Batch != nil {
				for table := range cur.Batch.Data.Keys() {
					if _, ok := f.accept.Get(table); !ok {
						cur.Batch.Data.Delete(table)
					}
				}
				if cur.Batch.Data.Len() == 0 {
					cur.Batch = nil
				}
			}
			select {
			case out <- cur:
			case <-ctx.Stopping():
				return nil
			}
			log.Infof("passed through %s", cur.Progress)
		}
	})
	return out, nil
}
