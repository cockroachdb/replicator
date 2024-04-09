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

package stdpool

import (
	"regexp"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
	"golang.org/x/mod/semver"
)

// For example:
//
//	CockroachDB CCL v23.1.17 (aarch64-apple-darwin21.2, ....)
//	CockroachDB CCL v24.1.0-alpha.5-dev-d45a65e08d45383aade2bcffdcdbe72a0cc279b1 (....)
var roachVerPattern = regexp.MustCompile(`^CockroachDB.* (v\d+\.\d+.\d+(-[^ ]+)?) `)

// CockroachSemver extracts the semantic version string from a cluster's
// reported version.
func CockroachSemver(version string) (string, bool) {
	found := roachVerPattern.FindStringSubmatch(version)
	if found == nil {
		return "", false
	}
	return found[1], true
}

// CockroachMinVersion returns true if the CockroachDB version string is
// at least the specified minimum.
func CockroachMinVersion(version string, minVersion string) (bool, error) {
	found, ok := CockroachSemver(version)
	if !ok {
		return false, errors.Errorf("could not extract semver from %q", version)
	}
	if !semver.IsValid(found) {
		return false, errors.Errorf("not a semver: %q", found)
	}
	if !semver.IsValid(minVersion) {
		return false, errors.Errorf("not a semver: %q", minVersion)
	}
	return semver.Compare(found, minVersion) >= 0, nil
}

// cockroachSupportsNoFullScanHint returns true for CRDB >= v23.1.17 or
// CRDB >= v23.2.3.
//
// https://github.com/cockroachdb/cockroach/pull/119104
func cockroachSupportsNoFullScanHint(version string) (bool, error) {
	if ok, err := CockroachMinVersion(version, "v23.2.3"); err != nil {
		return false, err
	} else if ok {
		return true, nil
	}
	if isV232, err := CockroachMinVersion(version, "v23.2.0"); err != nil {
		return false, err
	} else if isV232 {
		return false, nil
	}
	return CockroachMinVersion(version, "v23.1.17")
}

func emptyHint(table ident.Table) *ident.Hinted[ident.Table] {
	return ident.WithHint(table, "")
}

func noFullScanHint(table ident.Table) *ident.Hinted[ident.Table] {
	return ident.WithHint(table, "@{NO_FULL_SCAN}")
}

// setTableHint will populate the [types.PoolInfo.HintNoFTS] field.
func setTableHint(info *types.PoolInfo) error {
	if info.Version == "" {
		return errors.New("set PoolInfo.Version first")
	}
	if info.Product != types.ProductCockroachDB {
		info.HintNoFTS = emptyHint
		return nil
	}
	ok, err := cockroachSupportsNoFullScanHint(info.Version)
	if err != nil {
		return err
	}
	if !ok {
		info.HintNoFTS = emptyHint
		return nil
	}
	info.HintNoFTS = noFullScanHint
	return nil
}
