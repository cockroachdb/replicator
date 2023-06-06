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

package cdc

// This file contains code repackaged from url.go.

import (
	"net/url"
	"regexp"

	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
)

// Example: /test/public/table/2020-04-04/202004042351304139680000000000000.RESOLVED
// Format is: /[targetDB]/[targetSchema]/[targetTable]/[date]/[timestamp].RESOLVED
var (
	resolvedQueryRegex = regexp.MustCompile(
		`^/(?P<targetDB>[^/]+)/(?P<targetSchema>[^/]+)/(?P<targetTable>[^/]+)/(?P<date>\d{4}-\d{2}-\d{2})/(?P<timestamp>\d{33}).RESOLVED$`)
	resolvedQueryDB        = resolvedQueryRegex.SubexpIndex("targetDB")
	resolvedQuerySchema    = resolvedQueryRegex.SubexpIndex("targetSchema")
	resolvedQueryTable     = resolvedQueryRegex.SubexpIndex("targetTable")
	resolvedQueryTimestamp = resolvedQueryRegex.SubexpIndex("timestamp")
)

func (h *Handler) parseResolvedQueryURL(url *url.URL, req *request) error {
	match := resolvedQueryRegex.FindStringSubmatch(url.Path)
	if len(match) != resolvedQueryRegex.NumSubexp()+1 {
		return errors.Errorf("can't parse url %s", url)
	}

	target := ident.NewTable(
		ident.New(match[resolvedQueryDB]),
		ident.New(match[resolvedQuerySchema]),
		ident.New(match[resolvedQueryTable]))

	tsText := match[resolvedQueryTimestamp]
	if len(tsText) != 33 {
		return errors.Errorf(
			"expected timestamp to be 33 characters long, got %d: %s",
			len(tsText), tsText,
		)
	}
	timestamp, err := parseResolvedTimestamp(tsText[:23], tsText[23:])
	if err != nil {
		return err
	}

	req.leaf = h.resolved
	req.target = target
	req.timestamp = timestamp
	return nil
}
