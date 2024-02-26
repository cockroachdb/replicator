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

package sequencer

import "github.com/cockroachdb/cdc-sink/internal/types"

// MarkingAcceptor is a marker interface to indicate that a
// [types.MultiAcceptor] will assume responsibility for calling
// [types.Stager.MarkApplied].
type MarkingAcceptor interface {
	// IsMarking should return true.
	IsMarking() bool
}

// IsMarking returns true if the acceptor implements [MarkingAcceptor]
// or [MarkingAcceptor] is implemented by an acceptor somewhere in a
// delegate chain.
func IsMarking(acc types.MultiAcceptor) bool {
	for acc != nil {
		if marker, ok := acc.(MarkingAcceptor); ok {
			return marker.IsMarking()
		}
		if wrapper, ok := acc.(interface{ Unwrap() types.MultiAcceptor }); ok {
			acc = wrapper.Unwrap()
		} else {
			return false
		}
	}
	return false
}
