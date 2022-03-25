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
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/target/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/stretchr/testify/assert"
)

func TestWatch(t *testing.T) {
	a := assert.New(t)

	// Override the delay to exercise the background goroutine.
	*RefreshDelay = time.Second
	defer func() { *RefreshDelay = time.Minute }()

	ctx, dbInfo, cancel := sinktest.Context()
	defer cancel()

	dbName, cancel, err := sinktest.CreateDB(ctx)
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
	a.Equal(time.Second, w.delay)

	ch, cancel, err := w.Watch(tblInfo.Name())
	if !a.NoError(err) {
		return
	}
	defer cancel()

	select {
	case <-time.After(2 * w.delay):
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
	case <-time.After(2 * w.delay):
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
	case <-time.After(2 * w.delay):
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

func createTableStatement(refs ...sinktest.TableInfo) string {
	stm := "CREATE TABLE %s (pk INT PRIMARY KEY "
	for i, ref := range refs {
		stm += ", fk_" + strconv.Itoa(i) + " INT REFERENCES " + ref.Name().String() + " (pk) "
	}
	stm += ")"
	fmt.Println(stm)
	return stm
}
func TestWatchFK(t *testing.T) {
	a := assert.New(t)
	*RefreshDelay = time.Second
	defer func() { *RefreshDelay = time.Minute }()

	ctx, dbInfo, cancel := sinktest.Context()
	defer cancel()

	dbName, cancel, err := sinktest.CreateDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	// Tables
	// Level 0. T1
	// Level 1. T2 -> T1
	// Level 2. T3 -> T1,T2
	// Level 3. T4 -> T1,T2,T3 ; T5 -> T1, T3
	// Level 4. T6 -> T5
	tbl1, err := sinktest.CreateTable(ctx, dbName, createTableStatement())
	if !a.NoError(err) {
		return
	}

	tbl2, err := sinktest.CreateTable(ctx, dbName, createTableStatement(tbl1))
	if !a.NoError(err) {
		return
	}

	tbl3, err := sinktest.CreateTable(ctx, dbName, createTableStatement(tbl1, tbl2))
	if !a.NoError(err) {
		return
	}

	tbl4, err := sinktest.CreateTable(ctx, dbName, createTableStatement(tbl1, tbl2, tbl3))
	if !a.NoError(err) {
		return
	}

	tbl5, err := sinktest.CreateTable(ctx, dbName, createTableStatement(tbl1, tbl3))
	if !a.NoError(err) {
		return
	}
	tbl6, err := sinktest.CreateTable(ctx, dbName, createTableStatement(tbl5))
	if !a.NoError(err) {
		return
	}

	w, cancel, err := newWatcher(ctx, dbInfo.Pool(), dbName)
	if !a.NoError(err) {
		return
	}
	defer cancel()
	a.Equal(time.Second, w.delay)

	snapshot := w.Snapshot()
	tables := snapshot.TablesSortedByFK
	// Expected T1, T2, T3,  T4 or T5, T5 or T4 , T6
	a.Equal(6, len(tables))
	tbl4Otbl5 := []string{tbl4.String(), tbl5.String()}
	a.Equal(tbl1.String(), tables[0].String())
	a.Equal(tbl2.String(), tables[1].String())
	a.Equal(tbl3.String(), tables[2].String())
	a.Contains(tbl4Otbl5, tables[3].String())
	a.Contains(tbl4Otbl5, tables[4].String())
	a.Equal(tbl6.String(), tables[5].String())

}
