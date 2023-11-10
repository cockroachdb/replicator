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

package debezium

import (
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
)

//go:generate go run golang.org/x/tools/cmd/stringer -type=operationType

// operationType represents the type of operation of an event from the debezium stream.
type operationType int

const (
	unknownOp operationType = iota
	changeSchemaOp
	deleteOp
	insertOp
	snapshotOp
	tombstoneOp
	updateOp
)

// opMapping maps the codes in the op field to an operationType
var opMapping = map[string]operationType{
	"c": insertOp,
	"d": deleteOp,
	"r": snapshotOp,
	"u": updateOp,
}

// sourceInfo collects metadata about each message in the debezium stream.
type sourceInfo struct {
	connector     string
	table         ident.Table
	operationType operationType
}

// message represents a mutation received in the debezium stream.
type message struct {
	mutation types.Mutation
	source   sourceInfo
}
