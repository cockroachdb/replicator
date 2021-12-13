// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemawatch

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/backend/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/stretchr/testify/assert"
)

func TestWatch(t *testing.T) {
	a := assert.New(t)

	watcherDelay = time.Second
	defer func() { watcherDelay = time.Minute }()

	ctx, dbInfo, cancel := sinktest.Context()
	defer cancel()

	dbName, cancel, err := sinktest.CreateDb(ctx)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	// Bootstrap column.
	tblInfo, err := sinktest.CreateTable(ctx, dbName, "CREATE TABLE %s (pk INT PRIMARY KEY)")
	if !a.NoError(err) {
		return
	}

	w, cancel, err := newWatcher(ctx, dbInfo.Pool(), dbName)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	ch, cancel, err := w.Watch(tblInfo.Name())
	if !a.NoError(err) {
		return
	}
	defer cancel()

	select {
	case <-time.After(10 * time.Second):
		a.FailNow("timed out waiting for channel data")
	case data := <-ch:
		if a.Len(data, 1) {
			a.Equal("pk", data[0].Name.Raw())
		}
	}

	// Add a column and expect to see it.
	if !a.NoError(retry.Execute(ctx, dbInfo.Pool(),
		fmt.Sprintf("ALTER TABLE %s ADD COLUMN v STRING", tblInfo.Name()))) {
		return
	}

	select {
	case <-time.After(10 * time.Second):
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
	case <-time.After(10 * time.Second):
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
