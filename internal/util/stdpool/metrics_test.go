// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stdpool_test

import (
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/sinktest/base"
	"github.com/cockroachdb/cdc-sink/internal/util/stdpool"
	"github.com/stretchr/testify/require"
)

// This test verifies that PublishMetrics can be called with the same
// destination host multiple times without panicking.
func TestConcurrentRegistration(t *testing.T) {
	r := require.New(t)

	fixture, cancel, err := base.NewFixture()
	r.NoError(err)
	defer cancel()

	c1 := stdpool.PublishMetrics(fixture.Pool)
	defer c1()

	c2 := stdpool.PublishMetrics(fixture.Pool)
	defer c2()
}
