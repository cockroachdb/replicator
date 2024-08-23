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

	"github.com/cockroachdb/field-eng-powertools/notify"
	"github.com/cockroachdb/field-eng-powertools/workgroup"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/applycfg"
	"github.com/cockroachdb/replicator/internal/util/crep"
	"github.com/cockroachdb/replicator/internal/util/diag"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/merge"
	"github.com/dop251/goja"
	"github.com/pkg/errors"
)

// replicationKey is a constant that we use to pass unreifiable deletion
// keys into the [DeletesTo] callback.
const (
	replicationKeyName  = "replicationKey"
	replicationKeyValue = "__REPLICATION_KEY__"
)

// DeleteKey is similar to Map in that it has the opportunity to modify
// or filter a mutation before subsequent processing. Rather than
// operating on [types.Mutation.Data], it should instead base its
// behavior on [types.Mutation.Key]. DeleteKey functions are internally
// synchronized to ensure single-threaded access to the underlying JS
// VM.
type DeleteKey func(ctx context.Context, mut types.Mutation) (types.Mutation, bool, error)

var identityDelete DeleteKey = func(_ context.Context, mut types.Mutation) (types.Mutation, bool, error) {
	return mut, true, nil
}

// A DeletesTo describes how a delete operation should be routed. It is
// analogous to a Dispatch, except that the input and output mutations
// are assumed to be delete-type (i.e. lacking a Data body).
type DeletesTo Dispatch

// A Dispatch function receives a source mutation and assigns mutations
// to some number of downstream tables. The defaultTable argument, if
// non-empty, indicates the table to which the mutation would be
// dispatched in the absence of a user-defined function. Dispatch
// functions are internally synchronized to ensure single-threaded
// access to the underlying JS VM.
type Dispatch func(
	ctx context.Context,
	defaultTable ident.Table,
	mutation types.Mutation,
) (*ident.TableMap[[]types.Mutation], error)

// dispatchTo returns a Dispatch which assigns all mutations to the
// given target.
func dispatchTo(target ident.Table) Dispatch {
	return func(_ context.Context, _ ident.Table, mut types.Mutation) (*ident.TableMap[[]types.Mutation], error) {
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
	// A user-provided function that routes deletes to zero or more
	// tables.
	DeletesTo DeletesTo `json:"-"`
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
	// A user-provided function to modify of filter mutations that
	// delete a row in the target table.
	DeleteKey DeleteKey `json:"-"`
	// A user-provided function to modify or filter mutations bound for
	// the target table.
	Map Map `json:"-"`
	// A user-defined TableAcceptor that may execute arbitrary SQL or
	// call [UserScript.Delegate].
	UserAcceptor types.TableAcceptor `json:"-"`
}

// errUserExec is used to interrupt the JS runtime.
var errUseExec = errors.New("UserScript.execJS() must be used when calling JS code")

// UserScript encapsulates a user-provided configuration expressed as a
// JavaScript program.
//
// NB: The single-threaded nature of JavaScript means that only a single
// goroutine may execute JS code at any given point in time. These
// critical sections are coordinated through the execJS method. It's
// also the case that the JS user code is tightly coupled to the
// particular [goja.Runtime] that loaded the script (e.g. JS global
// variables). Should contention on the runtime become problematic, the
// correct solution would be to create multiple Loaders that each
// evaluate a separate UserScript. The tradedoff here is that each
// instance of the userscript would then have distinct global variables.
type UserScript struct {
	Delegate types.TableAcceptor
	Sources  *ident.Map[*Source]
	Targets  *ident.TableMap[*Target]

	apiModule *goja.Object         // The Replicator JS module.
	rt        *goja.Runtime        // The JavaScript VM. See execJS.
	rtExit    notify.Var[struct{}] // Forms an ersatz event loop for checking promise status.
	rtMu      *sync.RWMutex        // Serialize access to the VM or its side-effects.
	tasks     *workgroup.Group     // Limits number of concurrent background tasks.
	target    ident.Schema         // The schema being populated.
	tracker   asyncTracker         // Assists async promise chains. See execTrackedJS.
	watcher   types.Watcher        // Access to target schema.
}

var (
	_ diag.Diagnostic          = (*UserScript)(nil)
	_ goja.AsyncContextTracker = (*UserScript)(nil)
)

// Diagnostic implements [diag.Diagnostic].
func (s *UserScript) Diagnostic(_ context.Context) any {
	return map[string]any{
		"sources": s.Sources,
		"targets": s.Targets,
	}
}

// Exited implements [goja.AsyncContextTracker]. If [UserScript.tracker]
// is non-nil, it will be exited and the reference cleared.
func (s *UserScript) Exited() {
	if trk := s.tracker; trk != nil {
		s.tracker = nil
		if err := trk.exit(s); err != nil {
			// This will be converted into a JS exception by the runtime.
			panic(err)
		}
	}
}

// Grab implements [goja.AsyncContextTracker]. The object returned from
// this method, an [asyncTracker], will be associated with any promise
// chains that may be created by the user script.
func (s *UserScript) Grab() any {
	return s.tracker
}

// Resumed implements [goja.AsyncContextTracker]. If the object is an
// [asyncTracker], its [asyncTracker.enter] method will be called with
// the receiver.
func (s *UserScript) Resumed(obj any) {
	if trk, ok := obj.(asyncTracker); ok {
		s.tracker = trk
		if err := trk.enter(s); err != nil {
			// This will be converted to a JS exception by the runtime.
			panic(err)
		}
	}
}

// bind validates the user configuration against the target schema and
// creates the public facade around JS callbacks.
func (s *UserScript) bind(loader *Loader) error {
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
			src.Dispatch = s.bindDispatch(sourceName, bag.Dispatch)

			if bag.DeletesTo == nil {
				return errors.Errorf("configureSource(%s) called with "+
					"a dispatch function and no deletesTo", sourceName)
			}

			// Convenience mechanism just to provide a single name.
			if justName, ok := bag.DeletesTo.Export().(string); ok {
				deletesTo, _, err := ident.ParseTableRelative(justName, s.target)
				if err != nil {
					return errors.Wrapf(err, "configureSource(%q).deletesTo", sourceName)
				}

				// Sanity-check that the table exists.
				if _, ok := s.watcher.Get().Columns.Get(deletesTo); !ok {
					return errors.Errorf(
						"configureSource(%q).deletesTo references a non-existent table %s",
						sourceName, deletesTo)
				}

				src.DeletesTo = bindDeletesToTable(deletesTo)
			} else {
				var userFn deletesToJS
				if err := s.rt.ExportTo(bag.DeletesTo, &userFn); err != nil {
					return errors.Wrapf(err, "configureSource(%q).deletesTo was not a function", sourceName)
				}
				src.DeletesTo = s.bindDeletesToFunction(sourceName, userFn)
			}

		case bag.Target != "":
			dest, _, err := ident.ParseTableRelative(bag.Target, s.target)
			if err != nil {
				return errors.Wrapf(err, "configureSource(%q).target", sourceName)
			}
			src.DeletesTo = bindDeletesToTable(dest)
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
		if bag.Apply != nil {
			// Using a Delegate does not preclude other options.
			tgt.UserAcceptor = newApplier(s, table, bag.Apply)
		}
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
		if bag.DeleteKey == nil {
			tgt.DeleteKey = identityDelete
		} else {
			tgt.DeleteKey = s.bindDeleteKey(table, bag.DeleteKey)
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
		if bag.Merge != nil {
			tgt.Merger, err = s.bindMerge(table, bag.Merge)
			if err != nil {
				return err
			}
		}
		for k, v := range bag.Ignore {
			if v {
				tgt.Ignore.Put(ident.New(k), true)
			}
		}
		tgt.RowLimit = bag.RowLimit
	}

	return nil
}

func (s *UserScript) bindDeleteKey(table ident.Table, deleteKey deleteKeyJS) DeleteKey {
	return func(ctx context.Context, mut types.Mutation) (types.Mutation, bool, error) {
		// Unpack key into slice.
		key, err := crep.Unmarshal(mut.Key)
		if err != nil {
			return types.Mutation{}, false, errors.WithStack(err)
		}
		keyArr, ok := key.([]crep.Value)
		if !ok {
			return types.Mutation{}, false, errors.New("mutation key was not an array")
		}
		meta := mut.Meta
		if meta == nil {
			meta = make(map[string]any)
		}

		var jsKeyLen int
		var keyBytes json.RawMessage
		if err := s.execJS(func(*goja.Runtime) error {
			jsKey, err := deleteKey(keyArr, meta)
			if err != nil {
				return err
			}
			jsKeyLen = len(jsKey)

			keyBytes, err = json.Marshal(jsKey)
			if err != nil {
				return errors.WithStack(err)
			}

			return err
		}); err != nil {
			return types.Mutation{}, false, err
		}

		// Allow delete to be elided.
		if jsKeyLen == 0 {
			return mut, false, nil
		}

		// Sanity check now, rather than in apply code.
		colData, ok := s.watcher.Get().Columns.Get(table)
		if !ok {
			return types.Mutation{}, false,
				errors.Errorf("deleteKey missing schema data for %s", table)
		}

		pkCount := 0
		for idx := range colData {
			if colData[idx].Primary {
				pkCount = idx + 1
			} else {
				break
			}
		}

		if jsKeyLen != pkCount {
			return types.Mutation{}, false,
				errors.Errorf("deleteKey function returned %d elements, but %s has %d PK columns",
					jsKeyLen, table, pkCount)
		}

		return types.Mutation{
			Key:  keyBytes,
			Time: mut.Time,
		}, true, nil

	}
}

// bindDeletesToFunction exports a user-provided function as a
// DeletesTo. Delete mutations don't have a Data field, but presenting
// arrays of the PK column values to the user doesn't have great
// ergonomics. We know the table that a delete would normally be
// assigned to and we'll cook up a sparse data payload. Because we're
// delegating to the existing bindDispatch plumbing, we also know that
// it will have already populated the mutation's key array. All we need
// to do here is to remove the Data slice.
func (s *UserScript) bindDeletesToFunction(fnName string, deletesTo deletesToJS) DeletesTo {
	delegate := s.bindDispatch(fnName, dispatchJS(deletesTo))

	return func(ctx context.Context, defaultTable ident.Table, mut types.Mutation) (*ident.TableMap[[]types.Mutation], error) {
		// Depending on the frontend, we may or may not have a data
		// block associated with a deletion. If we don't have a data
		// block, we'll see if the incoming table name maps onto a
		// target table for which we have schema information. If we do,
		// we'll cook up a complete document for the user function. If
		// we don't have target schema data, we'll pass the replication
		// key through as a sentinel value. In either case, the callback
		// will be able to provide us with a destination table.
		if !mut.HasData() {
			// Reify the mutation's key.
			value, err := crep.Unmarshal(mut.Key)
			if err != nil {
				return nil, err
			}
			arr, ok := value.([]crep.Value)
			if !ok {
				return nil, errors.Errorf("mutation key %s was not an array", mut.Key)
			}
			data := make(map[string]crep.Value, len(arr))
			if cols, ok := s.watcher.Get().Columns.Get(defaultTable); ok {
				// We have schema data for the incoming table name, so
				// we'll cook up a data object for the script.
				for idx, col := range cols {
					if !col.Primary {
						break
					}
					if idx >= len(arr) {
						return nil, errors.Errorf(
							"mutation key has %d elements, but table %s has at least %d PK columns",
							len(arr), defaultTable, idx)
					}
					// Sanitize key name, too.
					data[col.Name.Canonical().Raw()] = arr[idx]
				}
			} else {
				// No schema data, just pass through a sentinel value.
				data[replicationKeyValue] = arr
			}
			mut.Data, err = json.Marshal(data)
			if err != nil {
				return nil, err
			}
		}

		// Call the user function.
		dispatched, err := delegate(ctx, defaultTable, mut)
		if err != nil {
			return nil, err
		}

		// Ensure that any emitted mutation will be a deletion.
		_ = dispatched.Range(func(_ ident.Table, muts []types.Mutation) error {
			for idx := range muts {
				muts[idx].Deletion = true
			}
			return nil
		})

		return dispatched, nil
	}
}

// bindDeletesToTable returns a DeletesTo that applies all deletes to
// the given table.
func bindDeletesToTable(table ident.Table) DeletesTo {
	return func(_ context.Context, _ ident.Table, mut types.Mutation) (*ident.TableMap[[]types.Mutation], error) {
		mut.Deletion = true // Ensure delete-type.
		ret := &ident.TableMap[[]types.Mutation]{}
		ret.Put(table, []types.Mutation{mut})
		return ret, nil
	}
}

// bindDispatch exports a user-provided function as a Dispatch.
func (s *UserScript) bindDispatch(fnName string, dispatch dispatchJS) Dispatch {
	return func(_ context.Context, _ ident.Table, mut types.Mutation) (*ident.TableMap[[]types.Mutation], error) {
		// Unmarshal the mutation's data as a generic map.
		data, err := crep.Unmarshal(mut.Data)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		dataMap, ok := data.(map[string]crep.Value)
		if !ok {
			return nil, errors.New("mutation data was not an object")
		}

		// If there's diff data, make that available.
		var beforeMap map[string]crep.Value
		if len(mut.Before) > 0 {
			data, err := crep.Unmarshal(mut.Before)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			// The before value could be a json null token.
			if data != nil {
				beforeMap, ok = data.(map[string]crep.Value)
				if !ok {
					return nil, errors.Errorf("mutation before data was not an object: %T", data)
				}
			}
		}

		meta := mut.Meta
		if meta == nil {
			meta = make(map[string]any)
		}
		meta["before"] = beforeMap

		// Execute the user function to route the mutation.
		var dispatches map[string][]map[string]any
		if err := s.execJS(func(*goja.Runtime) (err error) {
			dispatches, err = dispatch(dataMap, meta)
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
					Before: mut.Before,
					Data:   dataBytes,
					Key:    keyBytes,
					Meta:   meta,
					Time:   mut.Time,
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
		data, err := crep.Unmarshal(mut.Data)
		if err != nil {
			return types.Mutation{}, false, errors.WithStack(err)
		}
		dataMap, ok := data.(map[string]crep.Value)
		if !ok {
			return types.Mutation{}, false, errors.New("mutation data was not an object")
		}

		// Execute the user code to return the replacement values.
		var rawMapped map[string]any
		if err := s.execJS(func(*goja.Runtime) (err error) {
			meta := mut.Meta
			if meta == nil {
				meta = make(map[string]any)
			}
			rawMapped, err = mapper(dataMap, meta)
			return err
		}); err != nil {
			return types.Mutation{}, false, err
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
			return types.Mutation{}, false, errors.WithStack(err)
		}

		// Refresh the primary-key values in the mutation.
		colData, ok := s.watcher.Get().Columns.Get(table)
		if !ok {
			return types.Mutation{}, false, errors.Errorf("map missing schema data for %s", table)
		}

		var jsKey []any
		for _, colData := range colData {
			if colData.Primary {
				keyVal, ok := mapped.Get(colData.Name)
				if !ok {
					return types.Mutation{}, false, errors.Errorf(
						"map document missing value for PK column %s", colData.Name)
				}
				jsKey = append(jsKey, keyVal)
			}
		}

		keyBytes, err := json.Marshal(jsKey)
		if err != nil {
			return types.Mutation{}, false, errors.WithStack(err)
		}

		return types.Mutation{
			Before: mut.Before,
			Data:   dataBytes,
			Key:    keyBytes,
			Time:   mut.Time,
		}, true, nil
	}
}

// bindMerge exports a user-provided function as a [merge.Func]. The merger value could
// be our reperesentation of [merge.Standard] or a JS function.
func (s *UserScript) bindMerge(table ident.Table, merger goja.Value) (merge.Merger, error) {
	s.rtMu.Lock()
	defer s.rtMu.Unlock()

	var ret merge.Merger
	wrapWithStandard := false

	// If the user called api.standardMerge(), we'll see an object
	// with a marker symbol.
	if obj := merger.ToObject(s.rt); obj.GetSymbol(symIsStandardMerge) != nil {
		// Unwrap the optional lambda: api.standardMerge(op => { ... } )
		merger = obj.GetSymbol(symMergeFallback)
		if merger == nil {
			// This was a no-args call to api.standardMerge(), so we
			// can just return the golang implementation.
			return &merge.Standard{}, nil
		}
		wrapWithStandard = true
	}

	// We either have merge: op => { ... } or an unwrapped fallback. In
	// either case, the wiring is the same. We'll make the js function
	// object available as our golang func binding.
	var jsMerger mergeJS
	if err := s.rt.ExportTo(merger, &jsMerger); err != nil {
		return nil, errors.Wrapf(err,
			"table %s: merge function does not conform to MergeFunction type", table)
	}

	// Create a merge.Func that invokes the user-provided JS.
	ret = merge.Func(func(ctx context.Context, con *merge.Conflict) (*merge.Resolution, error) {
		// con.Before will be nil in 2-way merge.
		if con.Target == nil {
			return nil, errors.New("nil value in Conflict.Target")
		}
		if con.Proposed == nil {
			return nil, errors.New("nil value in Conflict.Proposed")
		}

		// Execute the callback while holding a lock on the runtime to
		// ensure single-threaded access.
		var jsResult *mergeResult
		if err := s.execJS(func(rt *goja.Runtime) error {
			// Export the conflict as the js merge operation.
			op := &mergeOp{
				Meta:     con.Proposed.Meta,
				Target:   s.rt.NewDynamicObject(&bagWrapper{con.Target, rt}),
				Proposed: s.rt.NewDynamicObject(&bagWrapper{con.Proposed, rt}),
			}
			if con.Before != nil {
				op.Before = s.rt.NewDynamicObject(&bagWrapper{con.Before, rt})
			}
			if len(con.Unmerged) > 0 {
				unmerged := make([]any, len(con.Unmerged))
				for idx, ident := range con.Unmerged {
					unmerged[idx] = ident.Raw()
				}
				op.Unmerged = rt.NewArray(unmerged...)
			}

			// Invoke the JS by way of the golang func binding.
			var err error
			jsResult, err = jsMerger(op)
			return err
		}); err != nil {
			return nil, err
		}

		// Unpack the JS result object into a merge.Resolution. As with
		// the Resolution type, we require exactly one of the fields to
		// have a non-zero value.
		if jsResult == nil {
			return nil, errors.Errorf(
				"table %s: merge function did not return a MergeResult object", table.Raw())
		}
		if jsResult.Drop {
			return &merge.Resolution{Drop: true}, nil
		}
		// Copy the proposed data out to a DLQ.
		if jsResult.DLQ != "" {
			return &merge.Resolution{DLQ: jsResult.DLQ}, nil
		}
		// By the pigeonhole principle, the user made an error by
		// setting no fields.
		if jsResult.Apply == nil {
			return nil, errors.Errorf(
				"table %s: merge function did not return a well-formed MergeResult", table.Raw())
		}

		// See goja.Object.Export for discussion about the returned type.
		switch t := jsResult.Apply.Export().(type) {
		case *bagWrapper:
			// The user returned one of the input wrappers, so we can
			// just unwrap and return it.
			return &merge.Resolution{Apply: t.data}, nil

		case map[string]any:
			// The user returned a new JS object, likely by using an
			// object literal.
			out := merge.NewBagFrom(con.Proposed)
			for k, v := range t {
				out.Put(ident.New(k), v)
			}
			return &merge.Resolution{Apply: out}, nil

		default:
			return nil, errors.Errorf("table %s: unexpected type of apply value: %T", table.Raw(), t)
		}
	})

	if wrapWithStandard {
		ret = &merge.Standard{Fallback: ret}
	}
	return ret, nil
}

// await is a helper method to safely wait for a promise to be resolved
// or rejected. The JS runtime will update the promise from whichever
// goroutine is executing within execJS, so we can only read the promise
// state when no JS code is running.
func (s *UserScript) await(ctx context.Context, promise *goja.Promise) (goja.Value, error) {
	for {
		// We only need a read barrier for any updates that were made
		// during the last call to execJS.
		s.rtMu.RLock()
		// The wake channel will be closed whenever the next time execJS
		// exits. This forms an ersatz event loop.
		_, wake := s.rtExit.Get()
		// Read the promise state.
		state := promise.State()
		value := promise.Result()
		s.rtMu.RUnlock()

		switch state {
		case goja.PromiseStateFulfilled:
			// Success.
			return value, nil
		case goja.PromiseStateRejected:
			// Return the JS error as a golang error.
			return nil, errors.Errorf("userscript promise rejected: %v", value)
		case goja.PromiseStatePending:
			// Wait for the next time execJS is finished or for context
			// cancellation.
			select {
			case <-wake:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		default:
			return nil, errors.Errorf("unimplemented: %v", state)
		}
	}
}

// execJS is a shortcut for execTrackedJS(nil, fn).
func (s *UserScript) execJS(fn func(rt *goja.Runtime) error) error {
	return s.execTrackedJS(nil, fn)
}

// execTrackedPromise runs a background goroutine, but limits the total
// number of active tasks to a reasonable value. The resolve callback is
// safe to call from any goroutine. The use of a function callback, as
// opposed to using a return value, allows the promise implementation to
// clean up any resources that may be used by the continuation. If the
// promise implementation returns an error, the promise will be
// rejected. The tracker reference may be nil.
func (s *UserScript) execTrackedPromise(
	tracker asyncTracker, fn func(resolve func(result any)) error,
) *goja.Promise {
	promise, resolve, reject := s.rt.NewPromise()

	safeResolve := func(result any) {
		// Ignoring error since callback returns nil.
		_ = s.execTrackedJS(tracker, func(rt *goja.Runtime) error {
			resolve(result)
			return nil
		})
	}
	safeReject := func(result any) {
		// Ignoring error since callback returns nil.
		_ = s.execTrackedJS(tracker, func(rt *goja.Runtime) error {
			reject(result)
			return nil
		})
	}

	if err := s.tasks.Go(func(context.Context) {
		if err := fn(safeResolve); err != nil {
			safeReject(err)
		}
	}); err != nil {
		safeReject(err)
	}

	return promise
}

// An asyncTracker receives entry and exit callbacks to configure the
// UserScript so that a chain of asynchronous callbacks (e.g. promises)
// may operate with a consistent ambient environment (e.g. database
// transaction).
type asyncTracker interface {
	enter(*UserScript) error
	exit(*UserScript) error
}

// execTrackedJS ensures that the callback has exclusive access to the
// JS VM. An asyncTracker may be provided to enable continuation-passing
// when resuming a promise callback or other async behavior. The VM will
// be left in an interrupted state to return a useful error message if
// execJS is not called. The rtExit variable will be notified when the
// callback has finished executing to create an event loop.
func (s *UserScript) execTrackedJS(
	tracker asyncTracker, fn func(rt *goja.Runtime) error,
) (err error) {
	start := time.Now()
	s.rtMu.Lock()
	scriptEntryWait.Observe(time.Since(start).Seconds())
	s.rt.ClearInterrupt()
	defer func() {
		s.rt.Interrupt(errUseExec)
		s.rtMu.Unlock()
		s.rtExit.Notify()
		scriptExecTime.Observe(time.Since(start).Seconds())
	}()

	if tracker != nil {
		s.tracker = tracker
		defer func() {
			if exitErr := tracker.exit(s); exitErr != nil && err == nil {
				// Only overwrite error if one is not already set.
				err = exitErr
			}
			s.tracker = nil
		}()
		if err := tracker.enter(s); err != nil {
			return err
		}
	}
	return fn(s.rt)
}
