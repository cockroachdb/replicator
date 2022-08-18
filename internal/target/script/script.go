// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package script contains support for loading configuration scripts
// built as JavaScript programs.
package script

import (
	"context"
	"encoding/json"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/target/apply"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/dop251/goja"
	esbuild "github.com/evanw/esbuild/pkg/api"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// A Dispatch function receives a source mutation and assigns mutations
// to some number of downstream tables.
type Dispatch func(ctx context.Context, mutation types.Mutation) (map[ident.Table][]types.Mutation, error)

// dispatchTo returns a Dispatch which assigns all mutations to the
// given target.
func dispatchTo(target ident.Table) Dispatch {
	return func(ctx context.Context, mut types.Mutation) (map[ident.Table][]types.Mutation, error) {
		return map[ident.Table][]types.Mutation{
			target: {mut},
		}, nil
	}
}

// Look on my Works, ye Mighty, and despair!
//   { doc } => { "target" : [ { doc }, ... ], "otherTarget" : [ { doc }, ... ], ... }
type dispatchJS func(map[string]interface{}) (map[string][]map[string]interface{}, error)

// A Map function may modify the mutations that are applied to a
// specific table. The boolean value will be false if the input
// mutation should be discarded.
type Map func(ctx context.Context, mut types.Mutation) (types.Mutation, bool, error)

var identity Map = func(ctx context.Context, mut types.Mutation) (types.Mutation, bool, error) {
	return mut, true, nil
}

// A simple mapping function.
//   { doc } => { doc }
type mapJS func(map[string]interface{}) (map[string]interface{}, error)

// A Source holds user-provided configuration options for a
// generic data-source.
type Source struct {
	// The table to apply incoming deletes to; this assumes that the
	// target schema is using FK's with ON DELETE CASCADE.
	DeletesTo ident.Table
	Mapper    Dispatch
}

// sourceJS is used in the API binding.
type sourceJS struct {
	DeletesTo string     `goja:"deletesTo"`
	Dispatch  dispatchJS `goja:"dispatch"`
	Target    string     `goja:"target"`
}

// A Target holds user-provided configuration options
// for a target table.
type Target struct {
	apply.Config
	Map Map
}

// targetJS is used in the API binding. The apply.Config.SourceNames
// field is ignored, since that can be taken care of by the Map
// function.
type targetJS struct {
	// Column names.
	CASColumns []string `goja:"cas"`
	// Column to duration.
	Deadlines map[string]string `goja:"deadlines"`
	// Column to SQL expression to pass through.
	Exprs map[string]string `goja:"exprs"`
	// Column name.
	Extras string `goja:"extras"`
	// Column names.
	Ignore map[string]bool `goja:"ignore"`
	// Mutation to mutation.
	Map mapJS `goja:"map"`
}

// UserScript encapsulates a user-provided configuration expressed as a
// JavaScript program.
type UserScript struct {
	Options map[string]interface{} // A JSON-esque collection of values.
	Sources map[ident.Ident]*Source
	Targets map[ident.Table]*Target

	fs           fs.FS                 // Used by require.
	modules      map[string]goja.Value // Keys are URLs.
	requireStack []*url.URL            // Allows relative import paths
	rt           *goja.Runtime         // The JavaScript VM.
	rtMu         sync.Mutex            // Serialize access to the VM.
	target       ident.Schema          // The schema being populated.
	watcher      types.Watcher         // Access to target schema.
}

// bindMap exports a user-provided function as a Map.
func (s *UserScript) bindMap(table ident.Table, fn mapJS) Map {
	return func(ctx context.Context, mut types.Mutation) (types.Mutation, bool, error) {
		s.rtMu.Lock()
		defer s.rtMu.Unlock()
		s.rt.ClearInterrupt()

		data := make(map[string]interface{})
		if err := json.Unmarshal(mut.Data, &data); err != nil {
			return mut, false, errors.WithStack(err)
		}

		jsData, err := fn(data)
		if err != nil {
			return mut, false, err
		}

		// Filtered out.
		if len(jsData) == 0 {
			return mut, false, nil
		}

		dataBytes, err := json.Marshal(jsData)
		if err != nil {
			return mut, false, errors.WithStack(err)
		}

		colData, ok := s.watcher.Snapshot(s.target)[table]
		if !ok {
			return mut, false, errors.Errorf("map missing schema data for %s", table)
		}

		var jsKey []interface{}
		for _, colData := range colData {
			if colData.Primary {
				keyVal, ok := jsData[colData.Name.Raw()]
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

// bindDispatch exports a user-provided function as a Dispatch.
func (s *UserScript) bindDispatch(fnName string, fn dispatchJS) Dispatch {
	return func(ctx context.Context, mut types.Mutation) (map[ident.Table][]types.Mutation, error) {
		s.rtMu.Lock()
		defer s.rtMu.Unlock()
		s.rt.ClearInterrupt()

		data := make(map[string]interface{})
		if err := json.Unmarshal(mut.Data, &data); err != nil {
			return nil, errors.WithStack(err)
		}

		jsData, err := fn(data)
		if err != nil {
			return nil, err
		}

		// Filtered out, return an empty map.
		if len(jsData) == 0 {
			return map[ident.Table][]types.Mutation{}, nil
		}

		// Serialize mutations back to JSON.
		ret := make(map[ident.Table][]types.Mutation, len(jsData))
		for tblName, jsDocs := range jsData {
			tbl := ident.NewTable(s.target.Database(), s.target.Schema(), ident.New(tblName))
			tblMuts := make([]types.Mutation, len(jsDocs))
			ret[tbl] = tblMuts
			for idx, jsDoc := range jsDocs {
				colData, ok := s.watcher.Snapshot(s.target)[tbl]
				if !ok {
					return nil, errors.Errorf(
						"dispatch function %s returned unknown table %s", fnName, tbl)
				}

				// Extract the revised PK
				var jsKey []interface{}
				for _, col := range colData {
					if col.Primary {
						keyVal, ok := jsDoc[col.Name.Raw()]
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

// configureSource is exported to the JS runtime.
func (s *UserScript) configureSource(tableName string, bag *sourceJS) error {
	if (bag.Dispatch != nil) == (bag.Target != "") {
		return errors.Errorf("configureSource(%q): one of mapper or target must be set", tableName)
	}

	ret := &Source{}
	if bag.Dispatch != nil {
		if bag.DeletesTo != "" {
			ret.DeletesTo = ident.NewTable(
				s.target.Database(), s.target.Schema(), ident.New(bag.DeletesTo))
		}
		ret.Mapper = s.bindDispatch(tableName, bag.Dispatch)
	}
	if bag.Target != "" {
		dest := ident.NewTable(s.target.Database(), s.target.Schema(), ident.New(bag.Target))
		ret.DeletesTo = dest
		ret.Mapper = dispatchTo(dest)
	}

	s.Sources[ident.New(tableName)] = ret
	return nil
}

// configureTable is exported to the JS runtime.
func (s *UserScript) configureTable(tableName string, bag *targetJS) error {
	tgt := ident.NewTable(s.target.Database(), s.target.Schema(), ident.New(tableName))
	ret := &Target{Config: *apply.NewConfig()}
	for _, cas := range bag.CASColumns {
		ret.CASColumns = append(ret.CASColumns, ident.New(cas))
	}
	for k, v := range bag.Deadlines {
		d, err := time.ParseDuration(v)
		if err != nil {
			return errors.Wrapf(err, "configureTable(%q)", tableName)
		}
		ret.Deadlines[ident.New(k)] = d
	}
	for k, v := range bag.Exprs {
		ret.Exprs[ident.New(k)] = v
	}
	if bag.Extras != "" {
		ret.Extras = ident.New(bag.Extras)
	}
	if bag.Map == nil {
		ret.Map = identity
	} else {
		ret.Map = s.bindMap(tgt, bag.Map)
	}
	for k, v := range bag.Ignore {
		if v {
			ret.Ignore[ident.New(k)] = true
		}
	}

	s.Targets[tgt] = ret
	return nil
}

// require implements a basic version of the NodeJS-style require()
// function. The referenced module contents are loaded, converted to a
// version of ES supported by goja in CommonJS packaging, and then
// executed.
func (s *UserScript) require(module string) (goja.Value, error) {
	// Look for exact-match (e.g. the API import).
	if found, ok := s.modules[module]; ok {
		return found, nil
	}

	var err error
	var source *url.URL
	if len(s.requireStack) == 0 {
		source, err = url.Parse(module)
	} else {
		source, err = s.requireStack[len(s.requireStack)-1].Parse(module)
	}
	if err != nil {
		return nil, err
	}

	key := source.String()
	if found, ok := s.modules[key]; ok {
		return found, nil
	}

	s.requireStack = append(s.requireStack, source)
	defer func() { s.requireStack = s.requireStack[:len(s.requireStack)-1] }()

	log.Debugf("loading user script %s", source)

	var data []byte
	switch source.Scheme {
	case "file":
		f, err := s.fs.Open(source.Path[1:])
		if err != nil {
			return nil, errors.Wrap(err, source.Path)
		}
		defer f.Close()
		data, err = io.ReadAll(f)
		if err != nil {
			return nil, errors.Wrap(err, source.Path)
		}

	case "http", "https":
		resp, err := http.Get(source.String())
		if err != nil {
			return nil, err
		}
		data, err = io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}

	default:
		return nil, errors.Errorf("unsupported scheme %s", source.Scheme)
	}

	opts := esbuild.TransformOptions{
		Banner:     "((module, exports)=>{module.exports=exports;",
		Footer:     "return module.exports})({},{})",
		Format:     esbuild.FormatCommonJS,
		Loader:     esbuild.LoaderDefault,
		Sourcefile: key,
		Target:     esbuild.ES2015,
	}
	if strings.HasSuffix(key, ".js") || strings.HasSuffix(key, ".ts") {
		opts.Sourcemap = esbuild.SourceMapInline
	}
	res := esbuild.Transform(string(data), opts)

	if len(res.Errors) > 0 {
		strs := esbuild.FormatMessages(res.Errors, esbuild.FormatMessagesOptions{TerminalWidth: 80})
		for _, str := range strs {
			log.Error(str)
		}
		return nil, errors.New("could not transform source, see log messages for details")
	}
	prog, err := goja.Compile(key, string(res.Code), true)
	if err != nil {
		return nil, err
	}

	exports, err := s.rt.RunProgram(prog)
	if err != nil {
		return nil, err
	}
	s.modules[key] = exports
	return exports, nil
}

// setOptions is an escape-hatch for configuring dialects at runtime.
func (s *UserScript) setOptions(data map[string]interface{}) {
	s.Options = data
}
