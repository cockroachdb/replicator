// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package resolve

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Config is passed to New.
type Config struct {
	Appliers   types.Appliers
	MetaTable  ident.Table
	Pool       *pgxpool.Pool
	Stagers    types.Stagers
	Timekeeper types.TimeKeeper
	Watchers   types.Watchers
}

// Table is the usual choice for the timestamp table in normal operating
// conditions.
var Table = ident.NewTable(ident.StagingDB, ident.Public, ident.New("pending_timestamps"))

type factory struct {
	appliers   types.Appliers
	metaTable  ident.Table
	pool       *pgxpool.Pool
	stagers    types.Stagers
	timekeeper types.TimeKeeper
	watchers   types.Watchers

	noLoop bool // Set by test to disable starting resolve loops.

	mu struct {
		sync.RWMutex
		cleanup   []func()
		instances map[ident.Schema]*resolve
	}
}

var _ types.Resolvers = (*factory)(nil)

// New constructs a Resolver factory.
func New(ctx context.Context, cfg Config) (_ types.Resolvers, cancel func(), _ error) {
	if _, err := cfg.Pool.Exec(ctx, fmt.Sprintf(schema, cfg.MetaTable)); err != nil {
		return nil, func() {}, errors.WithStack(err)
	}

	f := &factory{
		appliers:   cfg.Appliers,
		metaTable:  cfg.MetaTable,
		pool:       cfg.Pool,
		stagers:    cfg.Stagers,
		timekeeper: cfg.Timekeeper,
		watchers:   cfg.Watchers,
	}
	f.mu.instances = make(map[ident.Schema]*resolve)

	// Run the bootstrap in a background context.
	bootstrapCtx, cancelBoot := context.WithCancel(context.Background())
	go f.bootstrapResolvers(bootstrapCtx)

	return f, func() {
		defer cancelBoot()

		f.mu.Lock()
		defer f.mu.Unlock()
		for _, fn := range f.mu.cleanup {
			fn()
		}
		f.mu.cleanup = nil
		f.mu.instances = make(map[ident.Schema]*resolve)
	}, nil
}

// Get implements types.Resolvers.
func (f *factory) Get(ctx context.Context, target ident.Schema) (types.Resolver, error) {
	if ret, ok := f.getUnlocked(target); ok {
		return ret, nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if found, ok := f.mu.instances[target]; ok {
		return found, nil
	}

	ret, err := newResolve(ctx,
		f.appliers, f.metaTable, f.pool, f.stagers, target,
		f.timekeeper, f.watchers)
	if err != nil {
		return nil, err
	}

	f.mu.instances[target] = ret
	if !f.noLoop {
		// Run the flush behavior in an isolated context.
		flushCtx, cancel := context.WithCancel(context.Background())
		f.mu.cleanup = append(f.mu.cleanup, cancel)
		go ret.loop(flushCtx)
	}

	return ret, nil
}

func (f *factory) getUnlocked(target ident.Schema) (types.Resolver, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	found, ok := f.mu.instances[target]
	return found, ok
}

// bootstrapResolvers ensures that there is an active resolve instance
// for every target schema listed in the metaTable. This ensures that,
// in a zero-incoming-traffic situation, previously-marked values will
// eventually be processed.
func (f *factory) bootstrapResolvers(ctx context.Context) {
	for {
		toEnsure, err := scanForTargetSchemas(ctx, f.pool, f.metaTable)
		if err != nil {
			log.WithError(err).Warn("could not scan for bootstrap schemas")
		}
		// toEnsure will be nil if there was an error.
		for _, schema := range toEnsure {
			if _, err := f.Get(ctx, schema); err != nil {
				log.WithField("schema", schema).WithError(err).Warn("could not bootstrap schema")
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Minute):
			// We can run this at a slow cycle, since other nodes will
			// create their resolve instances based on incoming traffic.
		}
	}
}
