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

package applycfg

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/replicator/internal/util/diag"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/stopper"
	"github.com/stretchr/testify/require"
)

func TestConfigs(t *testing.T) {
	r := require.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	diags := diag.New(stopper.WithContext(ctx))

	cfgs, err := ProvideConfigs(diags)
	r.NoError(err)

	tbl := ident.NewTable(ident.MustSchema(ident.New("db")), ident.New("table"))

	handle := cfgs.Get(tbl)
	zero, changed := handle.Get()
	r.NotNil(zero)
	r.True(zero.IsZero())

	r.NoError(cfgs.Set(tbl, &Config{
		Extras: ident.New("extras"),
	}))

	select {
	case <-changed:
	default:
		r.Fail("should have seen channel closed")
	}

	next, _ := handle.Get()
	r.True(ident.Equal(ident.New("extras"), next.Extras))

	r.NotNil(diags.Payload(ctx)["applycfg"])

	r.NoError(cfgs.Set(tbl, nil))
	zero, _ = handle.Get()
	r.True(zero.IsZero())
}
