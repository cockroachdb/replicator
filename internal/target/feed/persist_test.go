// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package feed

import (
	"testing"

	"github.com/cockroachdb/cdc-sink/internal/target/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/stretchr/testify/assert"
)

func TestPersistAndLoad(t *testing.T) {
	a := assert.New(t)
	ctx, dbInfo, cancel := sinktest.Context()
	a.NotEmpty(dbInfo.Version())
	defer cancel()

	dbName, cancel, err := sinktest.CreateDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer cancel()
	t.Log(dbName)

	feeds, cancel, err := New(ctx, dbInfo.Pool(), dbName)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	feed := complexFeed(ident.New("db"), ident.New("public"))
	if !a.NoError(feeds.Store(ctx, feed)) {
		return
	}

	// The call to Store above will have already triggered a refresh
	// cycle. We should expect to see the change reflected immediately.
	ch, cancel := feeds.Watch()
	defer cancel()
	found := <-ch
	// The primary types aren't comparable, but their persistent versions are.
	p1 := feed.marshalPayload()
	p2 := found[0].marshalPayload()
	a.Equal(p1, p2)
}
