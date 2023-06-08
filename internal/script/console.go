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
