// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package fslogical

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"

	"cloud.google.com/go/firestore"
	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/golang/groupcache/lru"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/option"
)

const (
	// This environment variable is used by the SDK.
	emulatorEnv = "FIRESTORE_EMULATOR_HOST"

	// GroupPrefix is applied to a source name that should be treated as
	// a Firestore collection group query. This allows all document
	// sub-collections with a given name to be queried as though they
	// were a single collection.
	GroupPrefix = "group:"
)

// Set by TestMain to allow documents to be cleared.
var enableWipe bool

// ProvideLoops is called by wire to construct a logical-replication
// loop for each configured collection/table pair.
func ProvideLoops(
	ctx context.Context,
	cfg *Config,
	loops *logical.Factory,
	fs *firestore.Client,
	st *Tombstones,
	userscript *script.UserScript,
) ([]*logical.Loop, func(), error) {
	if err := cfg.Preflight(); err != nil {
		return nil, nil, err
	}

	idx := 0
	ret := make([]*logical.Loop, len(userscript.Sources))
	for sourceName := range userscript.Sources {
		var q firestore.Query
		if r := sourceName.Raw(); strings.HasPrefix(r, GroupPrefix) {
			q = fs.CollectionGroup(r[len(GroupPrefix):]).Query
		} else {
			q = fs.Collection(r).Query
		}

		var err error
		ret[idx], err = loops.Get(ctx, sourceName.Raw(), &Dialect{
			backfillBatchSize: cfg.BackfillBatchSize,
			docIDProperty:     cfg.DocumentIDProperty.Raw(),
			fs:                fs,
			query:             q,
			tombstones:        st,
			sourceCollection:  sourceName,
			updatedAtProperty: cfg.UpdatedAtProperty,
		})
		if err != nil {
			return nil, nil, err
		}
		idx++
	}

	return ret, loops.Close, nil
}

// ProvideFirestoreClient is called by wire. If a local emulator is in
// use, the cleanup function will delete the test project data.
func ProvideFirestoreClient(ctx context.Context, cfg *Config) (*firestore.Client, func(), error) {
	// Project ID is usually baked into the JSON key file.
	projectID := firestore.DetectProjectID
	if cfg.ProjectID != "" {
		projectID = cfg.ProjectID
	}

	emulator := os.Getenv(emulatorEnv)
	if emulator == "" {
		client, err := firestore.NewClient(ctx,
			projectID,
			option.WithCredentialsFile(cfg.CredentialsFile))
		if err != nil {
			return nil, nil, err
		}
		return client, func() { _ = client.Close() }, nil
	}

	client, err := firestore.NewClient(ctx, projectID)
	if err != nil {
		return nil, nil, err
	}

	return client, func() {
		_ = client.Close()
		if enableWipe {
			wipeFirestore(emulator, projectID)
		}
	}, nil
}

// ProvideTombstones is called by wire to construct a helper that
// manages document tombstones.
func ProvideTombstones(
	ctx context.Context,
	cfg *Config,
	fs *firestore.Client,
	loops *logical.Factory,
	userscript *script.UserScript,
) (*Tombstones, error) {
	ret := &Tombstones{cfg: cfg}
	if cfg.TombstoneCollection == "" {
		log.Trace("no tombstone collection was configured")
		return ret, nil
	}

	ret.coll = fs.Collection(cfg.TombstoneCollection)
	ret.deletesTo = make(map[ident.Ident]ident.Table, len(userscript.Sources))
	for source, dest := range userscript.Sources {
		ret.deletesTo[source] = dest.DeletesTo
	}
	ret.source = ident.New(cfg.TombstoneCollection)
	ret.mu.cache = &lru.Cache{MaxEntries: 1_000_000}

	_, err := loops.Get(ctx, cfg.TombstoneCollection, ret)
	return ret, err
}

// Wipe any leftover documents from testing.
func wipeFirestore(host string, projectID string) {
	dest := &url.URL{
		Scheme: "http",
		Host:   host,
		Path:   fmt.Sprintf("/emulator/v1/projects/%s/databases/(default)/documents", projectID),
	}
	req, err := http.NewRequest(http.MethodDelete, dest.String(), http.NoBody)
	if err != nil {
		log.WithError(err).Error("could not clear firestore test db")
		return
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.WithError(err).Error("could not clear firestore test db")
		return
	}
	if resp.StatusCode == http.StatusOK {
		log.Tracef("cleared firestore emulator project %s", projectID)
	} else {
		log.Errorf("could not clear firestore test db: %d %s", resp.StatusCode, resp.Status)
	}
}
