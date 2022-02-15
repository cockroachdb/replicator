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
	"time"

	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/stretchr/testify/assert"
)

func TestCopy(t *testing.T) {
	a := assert.New(t)

	feed := complexFeed(ident.New("database"), ident.New("schema"))

	// The Feed type isn't directly comparable, but we can verify
	// that the marshaled versions are equivalent.
	a.Equal(feed.marshalPayload(), feed.Copy().marshalPayload())
}

// Also used in other tests..
func complexFeed(db, schema ident.Ident) *Feed {
	simpleTable := &Table{
		Target: ident.NewTable(db, schema, ident.New("simple_table")),
	}

	complexTable := &Table{
		Target: ident.NewTable(db, schema, ident.New("complex_table")),
	}

	complexTable.CAS = []*Column{
		complexTable.Column(ident.New("updated_at")),
		complexTable.Column(ident.New("version")),
	}
	for _, col := range complexTable.CAS {
		col.CAS = true
	}

	complexTable.Column(ident.New("known_but_boring")) // Should not appear in output.
	complexTable.Column(ident.New("expires_at")).Deadline = time.Hour
	complexTable.Column(ident.New("feed_name")).Target = ident.New("renamed_column")
	complexTable.Column(ident.New("expr")).Expression = "incoming_column + 1"
	s := complexTable.Column(ident.New("synthetic"))
	s.Expression = "gen_random_uuid()"
	s.Synthetic = true
	complexTable.Column(ident.New("updated_at")).Deadline = time.Minute

	return &Feed{
		Immediate: true,
		Name:      ident.New("my_feed"),
		Schema:    ident.NewSchema(db, schema),
		Tables: map[ident.Table]*Table{
			complexTable.Target: complexTable,
			simpleTable.Target:  simpleTable,
		},
		Version: 42,
	}
}
