// Copyright 2023 The Cockroach Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

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
	"github.com/cockroachdb/cdc-sink/internal/types"
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
	fs *firestore.Client,
	loops *logical.Factory,
	memo types.Memo,
	pool *types.StagingPool,
	st *Tombstones,
	userscript *script.UserScript,
) ([]*logical.Loop, func(), error) {
	if err := cfg.Preflight(); err != nil {
		return nil, nil, err
	}

	idx := 0
	ret := make([]*logical.Loop, userscript.Sources.Len())
	recurseFilter := &ident.Map[struct{}]{}

	err := userscript.Sources.Range(func(sourceName ident.Ident, source *script.Source) error {
		var sourcePath string
		var q firestore.Query
		if r := sourceName.Raw(); strings.HasPrefix(r, GroupPrefix) {
			sourcePath = r
			tail := r[len(GroupPrefix):]
			q = fs.CollectionGroup(tail).Query
			recurseFilter.Put(ident.New(tail), struct{}{})
		} else {
			coll := fs.Collection(r)
			sourcePath = coll.Path
			q = coll.Query
		}

		var err error
		ret[idx], err = loops.Get(ctx, &Dialect{
			backfillBatchSize: cfg.BackfillBatchSize,
			docIDProperty:     cfg.DocumentIDProperty.Raw(),
			fs:                fs,
			idempotent:        cfg.Idempotent,
			loops:             loops,
			memo:              memo,
			pool:              pool,
			query:             q,
			tombstones:        st,
			recurse:           source.Recurse,
			recurseFilter:     recurseFilter,
			sourceCollection:  sourceName,
			sourcePath:        sourcePath,
			updatedAtProperty: cfg.UpdatedAtProperty,
		}, logical.WithName(sourceName.Raw()))
		if err != nil {
			return err
		}
		idx++
		log.Infof("started firestore loop %s", sourceName)
		return nil
	})

	return ret, loops.Close, err
}

// ProvideFirestoreClient is called by wire. If a local emulator is in
// use, the cleanup function will delete the test project data.
// The UserScript is added as a fake dependency to ensure that any
// script-driven configuration is performed first.
func ProvideFirestoreClient(
	ctx context.Context, cfg *Config, _ *script.UserScript,
) (*firestore.Client, func(), error) {
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
	ret.deletesTo = &ident.Map[ident.Table]{}
	if err := userscript.Sources.Range(func(source ident.Ident, dest *script.Source) error {
		ret.deletesTo.Put(source, dest.DeletesTo)
		return nil
	}); err != nil {
		return nil, err
	}
	ret.source = ident.New(cfg.TombstoneCollection)
	ret.mu.cache = &lru.Cache{MaxEntries: 1_000_000}

	_, err := loops.Get(ctx, ret, logical.WithName(cfg.TombstoneCollection))
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
