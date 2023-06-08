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
	"fmt"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/google/wire"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideMemo,
	wire.Bind(new(types.Memo), new(*Memo)),
)

// ProvideMemo is called by Wire to construct the KV wrapper.
func ProvideMemo(
	ctx context.Context, db types.StagingPool, staging ident.StagingDB,
) (*Memo, error) {
	target := ident.NewTable(staging.Ident(), ident.Public, ident.New("memo"))
	if err := retry.Execute(ctx, db, fmt.Sprintf(schema, target)); err != nil {
		return nil, err
	}
	ret := &Memo{}
	ret.sql.get = fmt.Sprintf(getTemplate, target)
	ret.sql.update = fmt.Sprintf(updateTemplate, target)
	return ret, nil
}
