// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ident

// Value allows Idents to be used with the spf13 flags package.
type Value Ident

// NewValue wraps the given Ident so that it can be used with the spf13 flags package.
func NewValue(value string, id *Ident) *Value {
	*id = New(value)
	return (*Value)(id)
}

// Set implements Value.
func (v *Value) Set(s string) error { *(*Ident)(v) = New(s); return nil }

func (v *Value) String() string { return (*Ident)(v).Raw() }

// Type implements Value.
func (v *Value) Type() string { return "ident" }
