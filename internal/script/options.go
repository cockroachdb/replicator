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
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

// Options is an injection point for a value that will receive any
// additional configuration from api.setOptions.
type Options interface{ Set(key, value string) error }

// NoOptions always returns an error when invoked.
var NoOptions Options = &noOptions{}

type noOptions struct{}

// Set always returns an error.
func (o *noOptions) Set(_, _ string) error {
	return errors.New("no options are supported by this dialect")
}

// FlagOptions adapts a pflag.FlagSet to the Options interface.
type FlagOptions struct {
	Flags *pflag.FlagSet
}

// Set implements Options.
func (o *FlagOptions) Set(key, value string) error {
	f := o.Flags.Lookup(key)
	if f == nil {
		return errors.Errorf("unknown option %q", key)
	}
	return errors.Wrapf(f.Value.Set(value), "option %q", key)
}
