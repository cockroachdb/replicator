// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemawatch_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/sinktest/all"
	"github.com/cockroachdb/cdc-sink/internal/target/schemawatch"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/stretchr/testify/assert"
)

func TestWatch(t *testing.T) {
	a := assert.New(t)

	// Override the delay to exercise the background goroutine.
	const delay = time.Second
	*schemawatch.RefreshDelay = delay
	defer func() { *schemawatch.RefreshDelay = time.Minute }()

	fixture, cancel, err := all.NewFixture()
	if !a.NoError(err) {
		return
	}
	defer cancel()

	ctx := fixture.Context
	dbName := fixture.TestDB.Ident()
	w := fixture.Watcher

	// Bootstrap column.
	tblInfo, err := fixture.CreateTable(ctx, "CREATE TABLE %s (pk INT PRIMARY KEY)")
	if !a.NoError(err) {
		return
	}

	ch, cancel, err := w.Watch(tblInfo.Name())
	if !a.NoError(err) {
		return
	}
	defer cancel()

	select {
	case <-ctx.Done():
		a.FailNow("timed out waiting for channel data")
	case data := <-ch:
		if a.Len(data, 1) {
			a.Equal("pk", data[0].Name.Raw())
		}
	}

	// Add a column and expect to see it.
	if !a.NoError(retry.Execute(ctx, fixture.Pool,
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN v STRING", tblInfo.Name()))) {
		return
	}

	select {
	case <-ctx.Done():
		a.FailNow("timed out waiting for channel data")
	case data := <-ch:
		if a.Len(data, 2) {
			a.Equal("pk", data[0].Name.Raw())
			a.Equal("v", data[1].Name.Raw())
		}
	}

	// Expect the channel to close if the table is dropped.
	if !a.NoError(tblInfo.DropTable(ctx)) {
		return
	}
	select {
	case <-ctx.Done():
		a.FailNow("timed out waiting for channel close")
	case _, open := <-ch:
		a.False(open)
	}

	// Check that we error out quickly on unknown tables.
	ch, cancel, err = w.Watch(ident.NewTable(dbName, ident.Public, ident.New("blah")))
	a.Nil(ch)
	a.Nil(cancel)
	a.Error(err)
}
