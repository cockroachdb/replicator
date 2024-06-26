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

package bucket

import "errors"

// Errors that we expect from the providers
var (
	// ErrNoSuchBucket must be returned if the bucket is not found.
	ErrNoSuchBucket = errors.New("bucket not found")
	// ErrNoSuchKey must be returned if there is no object with the given key.
	ErrNoSuchKey = errors.New("key not found")
	// ErrTransient represent an error that can be retried.
	ErrTransient = errors.New("the operation causing the error may be retried")
	// ErrSkipAll signal that Walk can stop process the follow entries.
	ErrSkipAll = errors.New("skip the rest of objects")
)
