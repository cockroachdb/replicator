// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logical

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/cdc-sink/internal/target/apply/fan"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/jackc/pgx/v4/pgxpool"
)

// Factory supports uses cases where it is desirable to have multiple,
// independent logical loops that share common resources.
type Factory struct {
	appliers types.Appliers
	cfg      *Config
	fans     *fan.Fans
	memo     types.Memo
	pool     *pgxpool.Pool

	mu struct {
		sync.Mutex
		cancels []func()
		loops   map[string]*Loop
	}
}

// Close terminates all running loops and waits for them to shut down.
func (f *Factory) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, cancel := range f.mu.cancels {
		cancel()
	}
	for _, loop := range f.mu.loops {
		<-loop.Stopped()
	}
	f.mu.cancels = nil
	f.mu.loops = make(map[string]*Loop)
}

// Get constructs or retrieves the named Loop.
func (f *Factory) Get(ctx context.Context, name string, dialect Dialect) (*Loop, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if found, ok := f.mu.loops[name]; ok {
		return found, nil
	}

	cfg := f.cfg.Copy()
	if cfg.ConsistentPointKey == "" {
		cfg.ConsistentPointKey = name
	} else {
		cfg.ConsistentPointKey = fmt.Sprintf("%s-%s", cfg.ConsistentPointKey, name)
	}

	ret, cancel, err := ProvideLoop(ctx, f.appliers, cfg, dialect, f.fans, f.memo, f.pool)
	if err != nil {
		return nil, err
	}
	f.mu.loops[name] = ret
	f.mu.cancels = append(f.mu.cancels, cancel)
	return ret, nil
}
