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

// Package eventproc implements strategies to process events stored
// within an object store.
package eventproc

import "github.com/cockroachdb/field-eng-powertools/stopper"

// A Processor downloads a file from an object store
// at the named path and operates on the mutations stored in the file.
type Processor interface {
	// Process downloads a file at the named path, deserializes the
	// change events inside the file, and forwards them to a target.
	// Processing may happen within the same replicator instance or it
	// can be sent to another replicator, depending on the
	// implementation. A Process returns a bucket.ErrTransient if the
	// operation may be retried on a different processor instance or at
	// a later time.
	Process(ctx *stopper.Context, path string) error
}
