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

// Package bypass contains a trivial [sequencer.Sequencer]
// implementation which writes data directly to the configured acceptor.
package bypass

import (
	"github.com/cockroachdb/cdc-sink/internal/sequencer"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/notify"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
)

// Bypass is a trivial implementation of [sequencer.Sequencer] that
// writes through to the underlying acceptor.
type Bypass struct{}

var _ sequencer.Sequencer = (*Bypass)(nil)

// Start implements [sequencer.Sequencer]. The emitted stat will be a
// dummy value.
func (b *Bypass) Start(
	_ *stopper.Context, opts *sequencer.StartOptions,
) (types.MultiAcceptor, *notify.Var[sequencer.Stat], error) {
	ret := &notify.Var[sequencer.Stat]{}
	ret.Set(sequencer.NewStat(opts.Group, &ident.TableMap[hlc.Time]{}))
	return opts.Delegate, ret, nil
}
