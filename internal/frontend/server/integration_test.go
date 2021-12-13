package server

import (
	"net/url"
	"strings"
	"testing"
	"time"

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
	feedUrl := url.URL{
		Scheme: "http",
		Host:   srv.listener.Addr().String(),
		Path:   target.Name().Database().Raw(),
	}
	if strings.Contains(dbInfo.Version(), "v20.2.") || strings.Contains(dbInfo.Version(), "v21.1.") {
		feedUrl.Scheme = "experimental-http"
	}

	if !a.NoError(source.Exec(ctx,
		"CREATE CHANGEFEED FOR TABLE %s "+
			"INTO '"+feedUrl.String()+"' "+
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
