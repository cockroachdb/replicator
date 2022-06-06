// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package resolve

import (
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/target/apply"
	"github.com/cockroachdb/cdc-sink/internal/target/schemawatch"
	"github.com/cockroachdb/cdc-sink/internal/target/sinktest"
	"github.com/stretchr/testify/assert"
)

func TestResolve(t *testing.T) {
	a := assert.New(t)
	ctx, dbInfo, cancel := sinktest.Context()
	a.NotEmpty(dbInfo.Version())
	defer cancel()

	targetDB, cancel, err := sinktest.CreateDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	watchers, cancel := schemawatch.NewWatchers(dbInfo.Pool())
	defer cancel()

	appliers, cancel := apply.NewAppliers(watchers)
	defer cancel()

	panic("finish writing")
}
