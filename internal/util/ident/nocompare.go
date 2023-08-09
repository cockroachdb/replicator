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

//go:build !race

package ident

// This is used by a test to ensure that, in production, the identifier
// types occupy one word.
const expectedIdentWords = 1

// A noCompare struct can be added to a type to prevent it from being
// used as a comparable type (e.g. in a map key). An alternate version
// of this type (containing a slice) will be used if the "race" build
// tag is present, since that is how we execute our tests.
//
// This should be an external lint tool in a future state.
type noCompare struct{}
