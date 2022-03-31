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
	"time"

	"github.com/cockroachdb/cdc-sink/internal/util/ident"
)

const defaultRetryDelay = 10 * time.Second

// Config contains the configuration necessary for creating a
// replication connection. All field, other than TestControls, are
// mandatory.
type Config struct {
	// Place the configuration into fan mode, where mutations are
	// applied without waiting for transaction boundaries.
	Immediate bool
	// The name of the publication to attach to.
	Publication string
	// The replication slot to attach to.
	Slot string
	// Connection string for the source db.
	SourceConn string
	// The amount of time to sleep between replication-loop retries.
	// If zero, a default value will be used.
	RetryDelay time.Duration
	// Connection string for the target cluster.
	TargetConn string
	// The SQL database in the target cluster to write into.
	TargetDB ident.Ident
	// Additional controls for testing.
	TestControls *TestControls
}

// TestControls define a collection of testing hook points for injecting
// behavior in a testing scenario. The function callbacks in this type
// must be prepared to be called from arbitrary goroutines. All fields
// in this type are optional.
type TestControls struct {
	BreakOnDataTuple     func() bool
	BreakReplicationFeed func() bool
	BreakSinkFlush       func() bool
}
