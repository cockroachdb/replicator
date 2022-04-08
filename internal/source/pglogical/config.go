// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pglogical

import (
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/pkg/errors"
)

// Config contains the configuration necessary for creating a
// replication connection. All field, other than TestControls, are
// mandatory.
type Config struct {
	logical.Config

	// The name of the publication to attach to.
	Publication string
	// The replication slot to attach to.
	Slot string
	// Connection string for the source db.
	SourceConn string

	// Used in testing to inject errors during processing.
	withChaosProb float32
}

// Preflight updates the configuration with sane defaults or returns an
// error if there are missing options for which a default cannot be
// provided.
func (c *Config) Preflight() error {
	if err := c.Config.Preflight(); err != nil {
		return err
	}
	if c.Publication == "" {
		return errors.New("no publication name was configured")
	}
	if c.Slot == "" {
		return errors.New("no replication slot name was configured")
	}
	if c.SourceConn == "" {
		return errors.New("no source connection was configured")
	}
	return nil
}
