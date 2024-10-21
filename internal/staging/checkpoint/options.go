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

package checkpoint

// An Option to [Checkpoints.Start].
type Option interface {
	isOption()
}

type disableStream struct{}

func (disableStream) isOption() {}

// DisableStream prevents the checkpointer from using a changefeed to
// receive change notifications from other instances of Replicator.
func DisableStream() Option {
	return disableStream{}
}

type limitLookahead int

// LimitLookahead limits the number of resolved timestamps that are used
// to calculate the extent of the resolving range.
func LimitLookahead(limit int) Option {
	return limitLookahead(limit)
}

func (l limitLookahead) isOption() {}

type skipBackwardsDataCheck struct{}

// SkipBackwardsDataCheck disables the error check for when
// data is moving backwards.
func SkipBackwardsDataCheck() Option {
	return skipBackwardsDataCheck{}
}

func (s skipBackwardsDataCheck) isOption() {}
