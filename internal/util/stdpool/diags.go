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

package stdpool

import (
	"context"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/diag"
)

// WithDiagnostics attaches information about the pool to the given
// [diag.Diagnostics] collector.
func WithDiagnostics(diags *diag.Diagnostics, label string) Option {
	return &withDiagnostics{diags, label}
}

type withDiagnostics struct {
	diags *diag.Diagnostics
	label string
}

var _ poolInfoOption = (*withDiagnostics)(nil)

func (o *withDiagnostics) option() {}

func (o *withDiagnostics) poolInfo(_ context.Context, info *types.PoolInfo) error {
	return o.diags.Register(o.label, diag.DiagnosticFn(func(ctx context.Context) any {
		return info
	}))
}
