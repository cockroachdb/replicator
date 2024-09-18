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

package workload

import (
	"fmt"

	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/pkg/errors"
)

// Checker is an abstraction over validating the data that was emitted
// by a [GeneratorBase].
type Checker struct {
	*GeneratorBase
	LoadChild    func(id int) (parent int, val int64, ok bool, err error)
	LoadParent   func(id int) (val int64, ok bool, err error)
	RowCounter   func(table ident.Table) (int, error)
	StageCounter func(table ident.Table, rng hlc.Range) (int, error)
}

// CheckConsistent validates the expected state within the generator to
// the relevant external store.
// It checks after the mutations / txns being applied on the target.
func (c *Checker) CheckConsistent() (failures []string, _ error) {
	failf := func(msg string, args ...any) {
		failures = append(failures, fmt.Sprintf(msg, args...))
	}

	if counter := c.StageCounter; counter != nil {
		if count, err := counter(c.Parent, c.Range()); err != nil {
			return nil, errors.Wrap(err, "parent staged count")
		} else if count != 0 {
			failf("unapplied mutations in parent table %s", c.Parent)
		}

		if count, err := counter(c.Child, c.Range()); err != nil {
			return nil, errors.Wrap(err, "child staged count")
		} else if count != 0 {
			failf("unapplied mutations in child table %s", c.Child)
		}
	}

	if counter := c.RowCounter; counter != nil {
		if count, err := counter(c.Parent); err != nil {
			return nil, errors.Wrap(err, "could not count parent table")
		} else if count != len(c.Parents) {
			failf("expected %d parents, got %d", len(c.Parents), count)
		}

		if count, err := counter(c.Child); err != nil {
			return nil, errors.Wrap(err, "could not count child table")
		} else if count != len(c.Children) {
			failf("expected %d children, got %d", len(c.Children), count)
		}
	}

	if loader := c.LoadParent; loader != nil {
		for parent, expectedVal := range c.ParentVals {
			foundVal, ok, err := loader(parent)
			if err != nil {
				return nil, errors.Errorf("could not load parent %d", parent)
			}
			if !ok {
				failf("parent %d not found", parent)
				continue
			}
			if foundVal != expectedVal {
				failf("parent %d: expected %d, got %d", parent, expectedVal, foundVal)
			}
		}
	}

	if loader := c.LoadChild; loader != nil {
		for child, expectedVal := range c.ChildVals {
			foundParent, foundVal, ok, err := loader(child)
			if err != nil {
				return nil, errors.Errorf("could not load parent %d", child)
			}
			if !ok {
				failf("parent %d not found", child)
				continue
			}
			if foundVal != expectedVal {
				failf("parent %d: expected %d, got %d", child, expectedVal, foundVal)
			}
			expectedParent := c.ChildToParent[child]
			if foundParent != expectedParent {
				failf("child %d: expected parent %d, got %d", child, expectedParent, foundParent)
			}
		}
	}

	return failures, nil
}
