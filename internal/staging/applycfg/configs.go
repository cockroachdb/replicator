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

// Package applycfg contains code for persisting applier configurations.
// This is separate from apply since we use the staging database as the
// storage location.
package applycfg

import (
	"context"
	"sync"

	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/notify"
)

// Configs provides a lookup service for per-destination-table
// configurations.
type Configs struct {
	mu struct {
		sync.RWMutex
		data ident.TableMap[*notify.Var[*Config]]
	}
}

// Diagnostic implements [diag.Diagnostic].
func (c *Configs) Diagnostic(_ context.Context) any {
	c.mu.RLock()
	defer c.mu.RUnlock()

	ret := &ident.TableMap[*Config]{}
	// No error returned from callback.
	_ = c.mu.data.Range(func(tbl ident.Table, v *notify.Var[*Config]) error {
		cfg, _ := v.Get()
		ret.Put(tbl, cfg)
		return nil
	})

	return ret
}

// Get returns a handle to the configuration for the named table. If the
// table has not (yet) been configured, the handle will contain a
// non-nil Config.
func (c *Configs) Get(tbl ident.Table) *notify.Var[*Config] {
	c.mu.RLock()
	found, ok := c.mu.data.Get(tbl)
	c.mu.RUnlock()
	if ok {
		return found
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Double-check with stronger lock.
	if found, ok := c.mu.data.Get(tbl); ok {
		return found
	}

	ret := &notify.Var[*Config]{}
	ret.Set(NewConfig())
	c.mu.data.Put(tbl, ret)
	return ret
}

// Set updates the active configuration for the given table.
func (c *Configs) Set(tbl ident.Table, cfg *Config) error {
	// Treat nil as zero so we don't break the contract in Get().
	if cfg == nil {
		cfg = NewConfig()
	}

	// Get and Set are both synchronized, so we don't need any other
	// locking here.
	c.Get(tbl).Set(cfg)

	// This method returns error so that we could easily add some
	// additional validation in the future without worrying about
	// existing callsites.
	return nil
}
