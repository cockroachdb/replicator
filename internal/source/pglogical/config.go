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

import "github.com/cockroachdb/cdc-sink/internal/util/ident"

// Config contains the configuration necessary for creating a
// replication connection. All field, other than TestControls, are
// mandatory.
type Config struct {
	// The name of the publication to attach to.
	Publication string
	// The replication slot to attach to.
	Slot string
	// Connection string for the source db.
	SourceConn string
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
	BreakReplicationFeed func() bool
	BreakSinkFlush       func() bool
	BreakSinkStage       func() bool
}
