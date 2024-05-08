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

package jwt

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/stopper"
	"github.com/google/wire"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Set is used by Wire.
var Set = wire.NewSet(ProvideAuth)

// ProvideAuth is called by Wire to construct a JWT-based authenticator.
// This provider will also start a background goroutine to look for
// configuration changes in the database.
func ProvideAuth(
	ctx *stopper.Context, db types.StagingQuerier, stagingDB ident.StagingSchema,
) (auth types.Authenticator, err error) {
	keyTable := ident.NewTable(stagingDB.Schema(), PublicKeysTable)
	revokedTable := ident.NewTable(stagingDB.Schema(), RevokedIdsTable)

	// Boostrap the schema.
	if _, err = db.Exec(ctx, fmt.Sprintf(ensureKeysTemplate, keyTable)); err != nil {
		err = errors.WithStack(err)
		return
	}
	if _, err = db.Exec(ctx, fmt.Sprintf(ensureRevokedTemplate, revokedTable)); err != nil {
		err = errors.WithStack(err)
		return
	}

	impl := &authenticator{}
	impl.sql.selectKeys = fmt.Sprintf(selectKeysTemplate, keyTable)
	impl.sql.selectRevoked = fmt.Sprintf(selectRevokedTemplate, revokedTable)

	// Initial data load also sets up fields in the mu struct.
	if err = impl.refresh(ctx, db); err != nil {
		return
	}

	// Start a refresh loop that will also listen for HUP signals.
	if *RefreshDelay > 0 {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGHUP)
		ctx.Go(func() error {
			defer close(ch)
			defer signal.Stop(ch)

			for {
				select {
				case <-ctx.Stopping():
					return nil
				case <-ch:
					log.Debug("reloading JWT data due to SIGHUP")
				case <-time.After(*RefreshDelay):
				}
				if err := impl.refresh(ctx, db); err != nil {
					log.WithError(err).Warn("could not refresh JWT data")
				}
			}
		})
	}

	auth = impl
	return
}
