// Copyright 2024 The Cockroach Authors
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

package ident

// Hinted decorates the raw and sql-safe string representations of an
// Identifier with an extra, target-specific hint string.
type Hinted[I Identifier] struct {
	Base I
	Hint string
}

// WithHint wraps the identifier with a target-specific hint. An empty
// hint string is valid.
func WithHint[I Identifier](id I, hint string) *Hinted[I] {
	return &Hinted[I]{id, hint}
}

// Raw implements [Identifier] and concatenates the underlying raw
// representation with the hint.
func (h *Hinted[I]) Raw() string {
	return h.Base.Raw() + h.Hint
}

// String implements [Identifier] and concatenates the underlying
// sql-safe representation with the hint.
func (h *Hinted[I]) String() string {
	return h.Base.String() + h.Hint
}
