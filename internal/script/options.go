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
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

// Options is an injection point for a FlagSet that will receive
// any additional configuration from api.setOptions.
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
