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
	"context"
	"strconv"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/pkg/errors"
)

// This is the timestamp format:  YYYYMMDDHHMMSSNNNNNNNNNLLLLLLLLLL
// Formatting const stolen from https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/changefeedccl/sink_cloudstorage.go#L48
const timestampDateTimeFormat = "20060102150405"

func parseResolvedTimestamp(timestamp string, logical string) (hlc.Time, error) {
	if len(timestamp) != 23 {
		return hlc.Time{}, errors.Errorf("can't parse timestamp %s", timestamp)
	}
	if len(logical) != 10 {
		return hlc.Time{}, errors.Errorf("can't parse logical timestamp %s", logical)
	}

	// Parse the date and time.
	timestampParsed, err := time.Parse(timestampDateTimeFormat, timestamp[0:14])
	if err != nil {
		return hlc.Time{}, err
	}

	// Parse out the nanos
	nanos, err := time.ParseDuration(timestamp[14:23] + "ns")
	if err != nil {
		return hlc.Time{}, err
	}
	timestampParsed = timestampParsed.Add(nanos)

	// Parse out the logical timestamp
	logicalParsed, err := strconv.Atoi(logical)
	if err != nil {
		return hlc.Time{}, err
	}

	return hlc.New(timestampParsed.UnixNano(), logicalParsed), nil
}

// resolved acts upon a resolved timestamp message.
func (h *Handler) resolved(ctx context.Context, req *request) error {
	_, resolver, err := h.Resolvers.get(ctx, req.target.Schema())
	if err != nil {
		return err
	}
	// In immediate mode, just log the incoming timestamp.
	if h.Config.Immediate {
		return resolver.Record(ctx, req.timestamp)
	}
	return resolver.Mark(ctx, req.timestamp)
}
