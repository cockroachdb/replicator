// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package apply

import (
	"github.com/cockroachdb/cdc-sink/internal/target/tblconf"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/google/wire"
)

// Set is used by Wire.
var Set = wire.NewSet(
	ProvideFactory,
)

// ProvideFactory is called by Wire to construct the factory. The cancel
// function will, in turn, destroy the per-schema types.Applier
// instances.
func ProvideFactory(configs *tblconf.Configs, watchers types.Watchers) (types.Appliers, func()) {
	f := &factory{
		configs:  configs,
		watchers: watchers,
	}
	f.mu.instances = make(map[ident.Table]*apply)
	return f, func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		for _, fn := range f.mu.cleanup {
			fn()
		}
		f.mu.cleanup = nil
		f.mu.instances = nil
	}
}
