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

// Package script contains support for loading configuration scripts
// built as JavaScript programs.
package script

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/applycfg"
	"github.com/cockroachdb/cdc-sink/internal/util/diag"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/dop251/goja"
	"github.com/pkg/errors"
)

// A Dispatch function receives a source mutation and assigns mutations
// to some number of downstream tables. Dispatch functions are
// internally synchronized to ensure single-threaded access to the
// underlying JS VM.
type Dispatch func(ctx context.Context, mutation types.Mutation) (*ident.TableMap[[]types.Mutation], error)

// dispatchTo returns a Dispatch which assigns all mutations to the
// given target.
func dispatchTo(target ident.Table) Dispatch {
	return func(_ context.Context, mut types.Mutation) (*ident.TableMap[[]types.Mutation], error) {
		ret := &ident.TableMap[[]types.Mutation]{}
		ret.Put(target, []types.Mutation{mut})
		return ret, nil
	}
}

// A Map function may modify the mutations that are applied to a
// specific table. The boolean value will be false if the input mutation
// should be discarded. Map functions are internally synchronized to
// ensure single-threaded access to the underlying JS VM.
type Map func(ctx context.Context, mut types.Mutation) (types.Mutation, bool, error)

var identity Map = func(_ context.Context, mut types.Mutation) (types.Mutation, bool, error) {
	return mut, true, nil
}

// A Source holds user-provided configuration options for a
// generic data-source.
type Source struct {
	// The table to apply incoming deletes to; this assumes that the
	// target schema is using FK's with ON DELETE CASCADE.
	DeletesTo ident.Table
	// A user-provided function that routes mutations to zero or more
	// tables.
	Dispatch Dispatch `json:"-"`
	// Enable recursion in sources which support nested sources.
	Recurse bool
}

// A Target holds user-provided configuration options
// for a target table.
type Target struct {
	applycfg.Config
	// A user-provided function to modify or filter mutations bound for
	// the target table.
	Map Map `json:"-"`
}

// UserScript encapsulates a user-provided configuration expressed as a
// JavaScript program.
type UserScript struct {
	Sources *ident.Map[*Source]
	Targets *ident.TableMap[*Target]

	rt      *goja.Runtime // The JavaScript VM. See execJS.
	rtMu    sync.Mutex    // Serialize access to the VM.
	target  ident.Schema  // The schema being populated.
	watcher types.Watcher // Access to target schema.
}

var _ diag.Diagnostic = (*UserScript)(nil)

// Diagnostic implements [diag.Diagnostic].
func (s *UserScript) Diagnostic(_ context.Context) any {
	return map[string]any{
		"sources": s.Sources,
		"targets": s.Targets,
	}
}

// bind validates the user configuration against the target schema and
// creates the public facade around JS callbacks.
func (s *UserScript) bind(loader *Loader) error {
	var err error
	// Evaluate calls to api.configureSource(). We implement a
	// last-one-wins approach if there are multiple calls for the same
	// source name.
	for sourceName, bag := range loader.sources {
		src := &Source{Recurse: bag.Recurse}
		// Note that this is not necessarily a SQL ident.
		s.Sources.Put(ident.New(sourceName), src)

		// The user has the option to provide either a dispatch function
		// or the name of a table.
		switch {
		case bag.Dispatch != nil:
			if bag.DeletesTo != "" {
				src.DeletesTo, _, err = ident.ParseTableRelative(bag.DeletesTo, s.target)
				if err != nil {
					return errors.Wrapf(err, "configureSource(%q).deletesTo", sourceName)
				}
			}
			src.Dispatch = s.bindDispatch(sourceName, bag.Dispatch)

		case bag.Target != "":
			dest, _, err := ident.ParseTableRelative(bag.Target, s.target)
			if err != nil {
				return errors.Wrapf(err, "configureSource(%q).target", sourceName)
			}
			src.DeletesTo = dest
			src.Dispatch = dispatchTo(dest)

		default:
			return errors.Errorf("configureSource(%q): dispatch or target required", sourceName)
		}
	}

	// Evaluate calls to api.configureTarget(). As above, we implement a
	// a last-one-wins approach.
	for tableName, bag := range loader.targets {
		table, _, err := ident.ParseTableRelative(tableName, s.target)
		if err != nil {
			return errors.Wrapf(err, "configureTable(%q)", tableName)
		}
		tgt := &Target{Config: *applycfg.NewConfig()}
		s.Targets.Put(table, tgt)

		for _, cas := range bag.CASColumns {
			tgt.CASColumns = append(tgt.CASColumns, ident.New(cas))
		}
		for k, v := range bag.Deadlines {
			d, err := time.ParseDuration(v)
			if err != nil {
				return errors.Wrapf(err, "configureTable(%q)", tableName)
			}
			tgt.Deadlines.Put(ident.New(k), d)
		}
		for k, v := range bag.Exprs {
			tgt.Exprs.Put(ident.New(k), v)
		}
		if bag.Extras != "" {
			tgt.Extras = ident.New(bag.Extras)
		}
		if bag.Map == nil {
			tgt.Map = identity
		} else {
			tgt.Map = s.bindMap(table, bag.Map)
		}
		for k, v := range bag.Ignore {
			if v {
				tgt.Ignore.Put(ident.New(k), true)
			}
		}
	}

	return nil
}

// bindDispatch exports a user-provided function as a Dispatch.
func (s *UserScript) bindDispatch(fnName string, dispatch dispatchJS) Dispatch {
	return func(ctx context.Context, mut types.Mutation) (*ident.TableMap[[]types.Mutation], error) {
		// Unmarshal the mutation's data as a generic map.
		data := make(map[string]any)
		if err := json.Unmarshal(mut.Data, &data); err != nil {
			return nil, errors.WithStack(err)
		}

		// Execute the user function to route the mutation.
		var dispatches map[string][]map[string]any
		if err := s.execJS(ctx, func() (err error) {
			meta := mut.Meta
			if meta == nil {
				meta = make(map[string]any)
			}
			dispatches, err = dispatch(data, meta)
			return err
		}); err != nil {
			return nil, err
		}

		ret := &ident.TableMap[[]types.Mutation]{}

		// If nothing returned, return an empty map.
		if len(dispatches) == 0 {
			return ret, nil
		}

		// Serialize mutations back to JSON.
		for tblName, jsDocs := range dispatches {
			tbl, _, err := ident.ParseTableRelative(tblName, s.target)
			if err != nil {
				return nil, errors.Wrapf(err,
					"dispatch function returned unparsable table name %q", tblName)
			}
			tblMuts := make([]types.Mutation, len(jsDocs))
			ret.Put(tbl, tblMuts)
			for idx, rawJsDoc := range jsDocs {
				// Use a case-insensitive map for lookups.
				jsDoc := &ident.Map[any]{}
				for k, v := range rawJsDoc {
					jsDoc.Put(ident.New(k), v)
				}

				colData, ok := s.watcher.Get().Columns.Get(tbl)
				if !ok {
					return nil, errors.Errorf(
						"dispatch function %s returned unknown table %s", fnName, tbl)
				}

				// Extract the revised primary key components.
				var jsKey []any
				for _, col := range colData {
					if col.Primary {
						keyVal, ok := jsDoc.Get(col.Name)
						if !ok {
							return nil, errors.Errorf(
								"dispatch funcion %s omitted value for PK %s", fnName, col.Name)
						}
						jsKey = append(jsKey, keyVal)
					}
				}

				dataBytes, err := json.Marshal(jsDoc)
				if err != nil {
					return nil, err
				}

				keyBytes, err := json.Marshal(jsKey)
				if err != nil {
					return nil, err
				}

				tblMuts[idx] = types.Mutation{
					Data: dataBytes,
					Key:  keyBytes,
					Time: mut.Time,
				}
			}
		}

		return ret, nil
	}
}

// bindMap exports a user-provided function as a Map func.
func (s *UserScript) bindMap(table ident.Table, mapper mapJS) Map {
	return func(ctx context.Context, mut types.Mutation) (types.Mutation, bool, error) {
		// Unpack data into generic map.
		data := make(map[string]any)
		if err := json.Unmarshal(mut.Data, &data); err != nil {
			return mut, false, errors.WithStack(err)
		}

		// Execute the user code to return the replacement values.
		var rawMapped map[string]any
		if err := s.execJS(ctx, func() (err error) {
			meta := mut.Meta
			if meta == nil {
				meta = make(map[string]any)
			}
			rawMapped, err = mapper(data, meta)
			return err
		}); err != nil {
			return mut, false, err
		}

		// Filtered out.
		if len(rawMapped) == 0 {
			return mut, false, nil
		}

		// Use case-insensitive idents for mapping.
		mapped := &ident.Map[any]{}
		for k, v := range rawMapped {
			mapped.Put(ident.New(k), v)
		}

		dataBytes, err := json.Marshal(mapped)
		if err != nil {
			return mut, false, errors.WithStack(err)
		}

		// Refresh the primary-key values in the mutation.
		colData, ok := s.watcher.Get().Columns.Get(table)
		if !ok {
			return mut, false, errors.Errorf("map missing schema data for %s", table)
		}

		var jsKey []any
		for _, colData := range colData {
			if colData.Primary {
				keyVal, ok := mapped.Get(colData.Name)
				if !ok {
					return mut, false, errors.Errorf(
						"map document missing value for PK column %s", colData.Name)
				}
				jsKey = append(jsKey, keyVal)
			}
		}

		keyBytes, err := json.Marshal(jsKey)
		if err != nil {
			return mut, false, errors.WithStack(err)
		}

		return types.Mutation{Data: dataBytes, Key: keyBytes, Time: mut.Time}, true, nil
	}
}

// execJS ensures that the callback has exclusive access to the JS VM.
// The JS execution will be interrupted when the context is canceled.
func (s *UserScript) execJS(ctx context.Context, fn func() error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	s.rtMu.Lock()
	s.rt.ClearInterrupt()
	go func() {
		<-ctx.Done()
		s.rt.Interrupt(ctx.Err())
		s.rtMu.Unlock()
	}()
	return fn()
}
