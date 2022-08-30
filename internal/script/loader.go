// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package script

import (
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"strings"

	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/dop251/goja"
	esbuild "github.com/evanw/esbuild/pkg/api"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// A JS function to dispatch source documents onto target tables.
//
// Look on my Works, ye Mighty, and despair!
//   { doc } => { "target" : [ { doc }, ... ], "otherTarget" : [ { doc }, ... ], ... }
type dispatchJS func(
	doc map[string]interface{},
	meta map[string]interface{},
) (map[string][]map[string]interface{}, error)

// A simple mapping function.
//   { doc } => { doc }
type mapJS func(
	doc map[string]interface{},
	meta map[string]interface{},
) (map[string]interface{}, error)

// sourceJS is used in the API binding.
type sourceJS struct {
	DeletesTo string     `goja:"deletesTo"`
	Dispatch  dispatchJS `goja:"dispatch"`
	Recurse   bool       `goja:"recurse"`
	Target    string     `goja:"target"`
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

// Loader is responsible for the first-pass execution of the user
// script. It will load all required resources, parse, and execute the
// top-level API calls.
type Loader struct {
	fs           fs.FS                     // Used by require.
	modules      map[string]goja.Value     // Keys are URLs.
	options      Options                   // Target of api.setOptions().
	requireStack []*url.URL                // Allows relative import paths
	rt           *goja.Runtime             // JS Runtime
	sources      map[string]*sourceJS      // User configuration.
	targets      map[ident.Ident]*targetJS // User configuration.
}

// configureSource is exported to the JS runtime.
func (l *Loader) configureSource(sourceName string, bag *sourceJS) error {
	if (bag.Dispatch != nil) == (bag.Target != "") {
		return errors.Errorf("configureSource(%q): one of mapper or target must be set", sourceName)
	}
	l.sources[sourceName] = bag
	return nil

}

// configureTable is exported to the JS runtime.
func (l *Loader) configureTable(tableName string, bag *targetJS) error {
	l.targets[ident.New(tableName)] = bag
	return nil
}

// require implements a basic version of the NodeJS-style require()
// function. The referenced module contents are loaded, converted to a
// version of ES supported by goja in CommonJS packaging, and then
// executed.
func (l *Loader) require(module string) (goja.Value, error) {
	// Look for exact-match (e.g. the API import).
	if found, ok := l.modules[module]; ok {
		return found, nil
	}

	var err error
	var source *url.URL
	if len(l.requireStack) == 0 {
		source, err = url.Parse(module)
	} else {
		source, err = l.requireStack[len(l.requireStack)-1].Parse(module)
	}
	if err != nil {
		return nil, err
	}

	key := source.String()
	if found, ok := l.modules[key]; ok {
		return found, nil
	}

	l.requireStack = append(l.requireStack, source)
	defer func() { l.requireStack = l.requireStack[:len(l.requireStack)-1] }()

	log.Debugf("loading user script %s", source)

	var data []byte
	switch source.Scheme {
	case "file":
		f, err := l.fs.Open(source.Path[1:])
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

	exports, err := l.rt.RunProgram(prog)
	if err != nil {
		return nil, err
	}
	l.modules[key] = exports
	return exports, nil
}

// setOptions is an escape-hatch for configuring dialects at runtime.
func (l *Loader) setOptions(data map[string]string) error {
	for k, v := range data {
		if err := l.options.Set(k, v); err != nil {
			return err
		}
	}
	return nil
}
