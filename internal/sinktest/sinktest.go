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

// Package sinktest contains utility types for writing cdc-sink tests.
package sinktest

import "github.com/cockroachdb/replicator/internal/util/ident"

// SourceSchema is an injection point that holds the name of a unique
// table schema in the source database in which to store user data.
type SourceSchema ident.Schema

// Schema returns the underlying database identifier.
func (t SourceSchema) Schema() ident.Schema { return ident.Schema(t) }

// TargetSchema is an injection point that holds the name of a unique
// table schema in the source database in which to store user data.
type TargetSchema ident.Schema

// Schema returns the underlying database identifier.
func (t TargetSchema) Schema() ident.Schema { return ident.Schema(t) }
