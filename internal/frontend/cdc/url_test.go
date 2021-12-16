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

	"github.com/stretchr/testify/assert"
)

func TestNdjsonURL(t *testing.T) {
	tcs := []struct {
		u         string
		expect    string
		expectErr bool
	}{
		{
			u:      "/db/public/2020-04-02/202004022058072107140000000000000-56087568dba1e6b8-1-72-00000000-test_table-1f.ndjson",
			expect: "db.public.test_table",
		},
		{
			u:      "/db/public/2020-04-02/202004022058072107140000000000000-56087568dba1e6b8-1-72-00000000-ignored_db.ignored_schema.test_table-1f.ndjson",
			expect: "db.public.test_table",
		},
		{
			u:         "/db/2020-04-02/202004022058072107140000000000000-56087568dba1e6b8-1-72-00000000-ignored_db.ignored_schema.test_table-1f.ndjson",
			expectErr: true,
		},
		{
			u:         "/2020-04-02/202004022058072107140000000000000-56087568dba1e6b8-1-72-00000000-ignored_db.ignored_schema.test_table-1f.ndjson",
			expectErr: true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.u, func(t *testing.T) {
			a := assert.New(t)
			p, err := parseNdjsonURL(tc.u)
			if tc.expectErr {
				a.Error(err)
				return
			}
			if a.NoError(err) {
				a.Equal(tc.expect, p.target.Raw())
			}
		})
	}
}

func TestResolvedURL(t *testing.T) {
	tcs := []struct {
		u         string
		expect    string
		expectErr bool
	}{
		{
			u:      "/db/public/2020-04-04/202004042351304139680000000000456.RESOLVED",
			expect: "db.public",
		},
		{
			u:         "/db/2020-04-04/202004042351304139680000000000456.RESOLVED",
			expectErr: true,
		},
		{
			u:         "/2020-04-04/202004042351304139680000000000456.RESOLVED",
			expectErr: true,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.u, func(t *testing.T) {
			a := assert.New(t)
			p, err := parseResolvedURL(tc.u)
			if tc.expectErr {
				a.Error(err)
				return
			}
			if a.NoError(err) {
				a.Equal(tc.expect, p.target.Raw())
			}
		})
	}
}
