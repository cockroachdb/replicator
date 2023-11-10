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

package debezium

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/httpauth"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

var _ http.Handler = (*Handler)(nil)

// Handler is an http.Handler for processing http requests
// from a debezium connector.
type Handler struct {
	Batcher       logical.Batcher
	Stop          *stopper.Context
	Config        *Config             // Runtime options.
	Authenticator types.Authenticator // Access checks.
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	sendErr := func(err error) {
		if err == nil {
			http.Error(w, "OK", http.StatusOK)
			return
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		log.WithError(err).WithField("uri", r.RequestURI).Error()
	}
	defer func() {
		if thrown := recover(); thrown != nil {
			err, ok := thrown.(error)
			if !ok {
				err = errors.Errorf("unexpected error: %v", thrown)
			}
			log.Error(err)
			sendErr(err)
		}
	}()

	allowed, err := h.checkAccess(ctx, r)
	switch {
	case err != nil:
		sendErr(err)
	case !allowed:
		http.Error(w, "missing or invalid access token", http.StatusUnauthorized)
	default:
		sendErr(h.handle(ctx, r))
	}
}

func (h *Handler) checkAccess(ctx context.Context, r *http.Request) (bool, error) {
	token := httpauth.Token(r)
	// It's OK if token is empty here, we might be using a trivial
	// Authenticator.
	ok, err := h.Authenticator.Check(ctx, h.targetSchema(), token)
	if err != nil {
		return false, err
	}
	if ok {
		return true, nil
	}
	return false, nil
}

func (h *Handler) handle(ctx context.Context, r *http.Request) error {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}

	switch strings.TrimSuffix(r.URL.Path, "/") {
	case "/sidecar":
		return h.handleSidecar(ctx, body)
	default:
		return errors.Errorf("%s not found", r.URL.Path)
	}
}

func (h *Handler) handleSidecar(ctx context.Context, body json.RawMessage) error {
	messages, err := parseBatch(body)
	if err != nil {
		return err
	}
	// Aggregate the mutations by target table.
	toProcess := &ident.TableMap[[]types.Mutation]{}
	for _, message := range messages {
		switch message.source.operationType {
		case insertOp, deleteOp, snapshotOp, updateOp:
			table := ident.NewTable(h.targetSchema(), message.source.table.Table())
			mut := message.mutation
			toProcess.Put(table, append(toProcess.GetZero(table), mut))
		default:
			log.Infof("skipping %s", message.source.operationType)
		}
	}
	return h.processMutationsImmediate(ctx, toProcess)
}

func (h *Handler) processMutationsImmediate(
	ctx context.Context, toProcess *ident.TableMap[[]types.Mutation],
) error {
	batch, err := h.Batcher.OnBegin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = batch.OnRollback(ctx) }()
	source := script.SourceName(h.targetSchema())
	if err := toProcess.Range(func(tbl ident.Table, muts []types.Mutation) error {
		for idx := range muts {
			// Index needed since it's not a pointer type.
			script.AddMeta("debezium", tbl, &muts[idx])
		}
		return batch.OnData(ctx, source, tbl, muts)
	}); err != nil {
		return err
	}

	select {
	case err := <-batch.OnCommit(ctx):
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *Handler) targetSchema() ident.Schema {
	return h.Config.TargetSchema
}
