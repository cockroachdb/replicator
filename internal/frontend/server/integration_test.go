// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"net/url"
	"strings"
	"testing"
	"time"

	"path"

	"github.com/cockroachdb/cdc-sink/internal/backend/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/stretchr/testify/assert"
)

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("short tests requested")
	}

	a := assert.New(t)
	ctx, dbInfo, cancel := sinktest.Context()
	defer cancel()

	sourceDB, cancel, err := sinktest.CreateDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	targetDB, cancel, err := sinktest.CreateDB(ctx)
	if !a.NoError(err) {
		return
	}
	defer cancel()

	// Prefer the webhook format for current versions of CRDB.
	useWebhook := true
	if strings.Contains(dbInfo.Version(), "v20.2.") || strings.Contains(dbInfo.Version(), "v21.1.") {
		useWebhook = false
	}

	*GenerateSelfSigned = useWebhook
	defer func() { *GenerateSelfSigned = false }()

	srv, err := newServer(ctx, "127.0.0.1:0", dbInfo.Pool().Config().ConnString(), false)
	if !a.NoError(err) {
		return
	}
	// Run the server loop in the background.
	go srv.serve()

	// Set up source and target tables.
	source, err := sinktest.CreateTable(ctx, sourceDB, "CREATE TABLE %s (pk INT PRIMARY KEY, val STRING)")
	if !a.NoError(err) {
		return
	}

	target := sinktest.NewTableInfo(dbInfo, ident.NewTable(targetDB, ident.Public, source.Name().Table()))
	if !a.NoError(target.Exec(ctx, "CREATE TABLE %s (pk INT PRIMARY KEY, val STRING)")) {
		return
	}

	// Add base data to the source table.
	a.NoError(source.Exec(ctx, "INSERT INTO %s (pk, val) VALUES (1, 'one')"))
	ct, err := source.RowCount(ctx)
	a.NoError(err)
	a.Equal(1, ct)

	// Set up the changefeed.
	var feedURL url.URL
	if useWebhook {
		feedURL = url.URL{
			Scheme:   "webhook-https",
			Host:     srv.listener.Addr().String(),
			Path:     path.Join(target.Name().Database().Raw(), target.Name().Schema().Raw()),
			RawQuery: "insecure_tls_skip_verify=true",
		}
	} else {
		feedURL = url.URL{
			Scheme: "experimental-http",
			Host:   srv.listener.Addr().String(),
			Path:   path.Join(target.Name().Database().Raw(), target.Name().Schema().Raw()),
		}
	}
	t.Logf("changefeed URL is %s", feedURL.String())
	if !a.NoError(source.Exec(ctx,
		"CREATE CHANGEFEED FOR TABLE %s "+
			"INTO '"+feedURL.String()+"' "+
			"WITH updated,resolved='1s'")) {
		return
	}

	// Wait for the backfilled value.
	for {
		ct, err := target.RowCount(ctx)
		if !a.NoError(err) {
			return
		}
		if ct >= 1 {
			break
		}
		t.Log("waiting for backfill")
		time.Sleep(time.Second)
	}

	// Insert an additional value
	a.NoError(source.Exec(ctx, "INSERT INTO %s (pk, val) VALUES (2, 'two')"))
	ct, err = source.RowCount(ctx)
	a.NoError(err)
	a.Equal(2, ct)

	// Wait for the streamed value.
	for {
		ct, err := target.RowCount(ctx)
		if !a.NoError(err) {
			return
		}
		if ct >= 2 {
			break
		}
		t.Log("waiting for stream")
		time.Sleep(time.Second)
	}
}
