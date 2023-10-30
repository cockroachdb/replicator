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

package script

import (
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"

	"github.com/dop251/goja"
	esbuild "github.com/evanw/esbuild/pkg/api"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// A JS function to dispatch source documents onto target tables.
//
// Look on my Works, ye Mighty, and despair!
//
//	{ doc } => { "target" : [ { doc }, ... ], "otherTarget" : [ { doc }, ... ], ... }
type dispatchJS func(
	doc map[string]any,
	meta map[string]any,
) (map[string][]map[string]any, error)

// A simple mapping function.
//
//	{ doc } => { doc }
type mapJS func(
	doc map[string]any,
	meta map[string]any,
) (map[string]any, error)

// A mergeOp is the input to the user-provided merge function.
type mergeOp struct {
	Before   goja.Value     `goja:"before"`   // Backed by bagWrapper. Nil in 2-way case.
	Meta     map[string]any `goja:"meta"`     // Equivalent to dispatch() or map() meta.
	Proposed goja.Value     `goja:"proposed"` // Backed by bagWrapper.
	Target   goja.Value     `goja:"target"`   // Backed by bagWrapper.
	Unmerged goja.Value     `goja:"unmerged"` // Intermediate state from standardMerge.
}

// A mergeResult is returned by the user-provided merge function.
// Exactly one field may be set to a non-zero value.
type mergeResult struct {
	Apply goja.Value `goja:"apply"` // The data to return
	DLQ   string     `goja:"dlq"`   // Append to a dead-letter queue.
	Drop  bool       `goja:"drop"`  // Discard the mutation.
}

type mergeJS func(*mergeOp) (*mergeResult, error)

// These symbols allow bindMerge and standardMerge to collude. We'd
// like to avoid an unnecessary round-trip through goja's value
// interface if we can call [merge.Standard] directly.
var (
	symIsStandardMerge = goja.NewSymbol("standardMerge") // Boolean flag
	symMergeFallback   = goja.NewSymbol("fallback")      // Reference to user-provided JS function.
)

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
	// Two- or three-way merge operator. The bindMerge method will
	// validate the type of value.
	Merge goja.Value `goja:"merge"`
}

// Loader is responsible for the first-pass execution of the user
// script. It will load all required resources, parse, and execute the
// top-level API calls.
type Loader struct {
	fs           fs.FS                 // Used by require.
	options      Options               // Target of api.setOptions().
	requireStack []*url.URL            // Allows relative import paths.
	requireCache map[string]goja.Value // Keys are URLs.
	rt           *goja.Runtime         // JS Runtime.
	rtMu         *sync.Mutex           // Serialize access to the VM.
	sources      map[string]*sourceJS  // User configuration.
	targets      map[string]*targetJS  // User configuration.
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
	l.targets[tableName] = bag
	return nil
}

// require is exported to the JS runtime and implements a basic version
// of the NodeJS-style require() function. The referenced module
// contents are loaded, converted to ES5 in CommonJS packaging, and then
// executed.
func (l *Loader) require(module string) (goja.Value, error) {
	// Look for an exact-match (e.g. the API import).
	if found, ok := l.requireCache[module]; ok {
		return found, nil
	}

	// The required path is parsed as a URL, relative to the top of the
	// require stack.  This allows, for example, a script to be loaded
	// from an external source which then refers to sibling paths.
	var err error
	var source *url.URL
	if len(l.requireStack) == 0 {
		// We bootstrap the runtime with require("file:///<main.js>")
		source, err = url.Parse(module)
	} else {
		parent := l.requireStack[len(l.requireStack)-1]
		source, err = parent.Parse(module)
		// This is a bit of a hack for .ts files, since their import
		// strings don't generally include the .ts extension.
		if err == nil && path.Ext(parent.Path) == ".ts" && path.Ext(source.Path) == "" {
			source.Path += ".ts"
		}
	}
	if err != nil {
		return nil, err
	}

	// At this point, the source is an absolute URL, so we'll use it
	// as the key.  We perform a second lookup to see if the external
	// module has been previously required.
	key := source.String()
	if found, ok := l.requireCache[key]; ok {
		return found, nil
	}

	// Push the script's location onto the stack, pop when we're done.
	l.requireStack = append(l.requireStack, source)
	defer func() { l.requireStack = l.requireStack[:len(l.requireStack)-1] }()

	log.Debugf("loading user script %s", source)

	// Acquire the contents of the script.  A file:// URL is loaded from
	// the supplied fs.FS, while http(s):// makes the relevant request.
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

	// These options will create a self-executing closure that provides
	// the expected ambient symbols for a CommonJS script. The header
	// assigns a stub object to the global __require_cache map to defuse
	// any cyclical module references. It then replaces that stub object
	// with the evaluated module exports to support resource imports.
	opts := esbuild.TransformOptions{
		Banner: fmt.Sprintf(`
__require_cache[%[1]q]=(()=>{
var exports = __require_cache[%[1]q] = {};
var module = {exports: exports};`, key),
		Footer:     "return module.exports;})()",
		Format:     esbuild.FormatCommonJS,
		Loader:     esbuild.LoaderDefault,
		Sourcefile: key,
		Target:     esbuild.ES2015,
	}
	// Source maps improve error messages from the JS runtime.
	if strings.HasSuffix(key, ".js") || strings.HasSuffix(key, ".ts") {
		opts.Sourcemap = esbuild.SourceMapInline
	}

	// Process the script or resource into the equivalent JS source.
	res := esbuild.Transform(string(data), opts)
	if len(res.Errors) > 0 {
		strs := esbuild.FormatMessages(res.Errors, esbuild.FormatMessagesOptions{TerminalWidth: 80})
		for _, str := range strs {
			log.Error(str)
		}
		return nil, errors.New("could not transform source, see log messages for details")
	}

	// Compile the source.
	prog, err := goja.Compile(key, string(res.Code), true)
	if err != nil {
		return nil, err
	}

	// Execute the program, which returns the module's exports. Note
	// that the assigment to l.requireCache happens via the
	// __require_cache binding in the script prelude.
	return l.rt.RunProgram(prog)
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

// standardMerge returns a JS object that [UserScript.bindMerge] will
// detect. This collusion allows us to avoid any goja wiring to pass the
// data into standardMerge.  Hopefully, the fallback won't need to be
// called, so we can perform the entire merge in go code.
func (l *Loader) standardMerge(jsFunc mergeJS) (*goja.Object, error) {
	ret := l.rt.NewObject()
	if err := ret.SetSymbol(symIsStandardMerge, true); err != nil {
		return nil, errors.WithStack(err)
	}
	if jsFunc != nil {
		if err := ret.SetSymbol(symMergeFallback, jsFunc); err != nil {
			return nil, errors.WithStack(err)
		}
	}
	return ret, nil
}
