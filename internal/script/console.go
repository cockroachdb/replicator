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
	"strings"

	"github.com/dop251/goja"
	log "github.com/sirupsen/logrus"
)

// console provides a trivial implementation of console logging to
// aid in debugging user scripts.
func console(rt *goja.Runtime) goja.Value {
	// This function signature avoids reflection.
	return rt.ToValue(map[string]func(call goja.FunctionCall) goja.Value{
		"debug": func(call goja.FunctionCall) goja.Value { return consoleLog(log.DebugLevel, call) },
		"info":  func(call goja.FunctionCall) goja.Value { return consoleLog(log.InfoLevel, call) },
		"log":   func(call goja.FunctionCall) goja.Value { return consoleLog(log.InfoLevel, call) },
		"trace": func(call goja.FunctionCall) goja.Value { return consoleLog(log.TraceLevel, call) },
		"warn":  func(call goja.FunctionCall) goja.Value { return consoleLog(log.WarnLevel, call) },
	})
}

// consoleLog is called by the functions bound in console.
func consoleLog(level log.Level, call goja.FunctionCall) goja.Value {
	if !log.IsLevelEnabled(level) {
		return goja.Undefined()
	}

	var sb strings.Builder
	for idx, arg := range call.Arguments {
		if idx > 0 {
			sb.WriteRune(' ')
		}
		sb.WriteString(arg.String())
	}
	log.StandardLogger().Log(level, sb.String())
	return goja.Undefined()
}
