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

package hlc

import "fmt"

// Range represents a half-open range of HLC values, inclusive of Min
// and exclusive of Max.
type Range [2]Time

// Empty returns true if the Min time is greater than or equal to the
// Max value.
func (r Range) Empty() bool { return Compare(r[0], r[1]) >= 0 }

// Min returns the inclusive, minimum value.
func (r Range) Min() Time { return r[0] }

// Max returns the exclusive, maximum value.
func (r Range) Max() Time { return r[1] }

func (r Range) String() string {
	return fmt.Sprintf("[ %s -> %s )", r[0], r[1])
}
