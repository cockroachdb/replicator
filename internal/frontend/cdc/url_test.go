// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cdc

// This file contains code repackaged from url_test.go.

import (
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/stretchr/testify/assert"
)

func TestNdjsonURL(t *testing.T) {
	a := assert.New(t)
	const u = "/target/2020-04-02/202004022058072107140000000000000-56087568dba1e6b8-1-72-00000000-test_table-1f.ndjson"

	p, err := parseNdjsonURL(u)
	if a.NoError(err) {
		a.Equal("target", p.targetDb)
		a.Equal("test_table", p.targetTable)
	}
}

func TestResolvedURL(t *testing.T) {
	a := assert.New(t)
	const u = "/target/2020-04-04/202004042351304139680000000000456.RESOLVED"

	r, err := parseResolvedURL(u)
	if a.NoError(err) {
		a.Equal("target", r.targetDb)
		a.Equal(hlc.New(1586044290000000000, 456), r.timestamp)
	}
}
