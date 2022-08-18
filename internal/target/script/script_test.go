// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package script_test

import (
	"context"
	"embed"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/target/apply"
	"github.com/cockroachdb/cdc-sink/internal/target/script"
	"github.com/cockroachdb/cdc-sink/internal/target/sinktest"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//go:embed testdata/*
var testData embed.FS

func TestScript(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)

	fixture, cancel, err := sinktest.NewFixture()
	r.NoError(err)
	defer cancel()

	ctx := fixture.Context

	// Create tables that will be referenced by the user-script.
	_, err = fixture.Pool.Exec(ctx,
		fmt.Sprintf("CREATE TABLE %s.table1(msg TEXT PRIMARY KEY)", fixture.TestDB.Ident()))
	r.NoError(err)
	_, err = fixture.Pool.Exec(ctx,
		fmt.Sprintf("CREATE TABLE %s.table2(idx INT PRIMARY KEY)", fixture.TestDB.Ident()))
	r.NoError(err)
	_, err = fixture.Pool.Exec(ctx,
		fmt.Sprintf("CREATE TABLE %s.all_features(msg TEXT PRIMARY KEY)", fixture.TestDB.Ident()))
	r.NoError(err)

	r.NoError(fixture.Watcher.Refresh(ctx, fixture.Pool))

	schema := ident.NewSchema(fixture.TestDB.Ident(), ident.Public)
	s, err := script.ProvideUserScript(
		fixture.Context,
		&script.Config{
			FS:       testData,
			MainPath: "/testdata/main.ts",
		},
		fixture.Configs,
		fixture.Pool,
		script.TargetSchema(schema),
		fixture.Watchers)
	r.NoError(err)
	a.Len(s.Sources, 2)
	a.Len(s.Targets, 2)
	a.Equal(map[string]interface{}{"hello": "world"}, s.Options)

	tbl1 := ident.NewTable(schema.Database(), schema.Schema(), ident.New("table1"))
	tbl2 := ident.NewTable(schema.Database(), schema.Schema(), ident.New("table2"))
	tblS := ident.NewTable(schema.Database(), schema.Schema(), ident.New("some_table"))

	if cfg := s.Sources[ident.New("expander")]; a.NotNil(cfg) {
		a.Equal(tbl1, cfg.DeletesTo)
		mut := types.Mutation{Data: []byte(`{"msg":true}`)}
		mapped, err := cfg.Mapper(context.Background(), mut)
		if a.NoError(err) && a.NotNil(mapped) {
			if docs := mapped[tbl1]; a.Len(docs, 1) {
				a.Equal(`{"dest":"table1","msg":true}`, string(docs[0].Data))
				a.Equal(`[true]`, string(docs[0].Key))
			}

			if docs := mapped[tbl2]; a.Len(docs, 2) {
				a.Equal(`{"dest":"table2","idx":0,"msg":true}`, string(docs[0].Data))
				a.Equal(`[0]`, string(docs[0].Key))

				a.Equal(`{"dest":"table2","idx":1,"msg":true}`, string(docs[1].Data))
				a.Equal(`[1]`, string(docs[1].Key))
			}
		}
	}

	if cfg := s.Sources[ident.New("passthrough")]; a.NotNil(cfg) {
		a.Equal(tblS, cfg.DeletesTo)
		mut := types.Mutation{Data: []byte(`{"passthrough":true}`)}
		mapped, err := cfg.Mapper(context.Background(), mut)
		if a.NoError(err) && a.NotNil(mapped) {
			tbl := ident.NewTable(schema.Database(), schema.Schema(), ident.New("some_table"))
			expanded := mapped[tbl]
			if a.Len(expanded, 1) {
				a.Equal(mut, expanded[0])
			}
		}
	}

	tbl := ident.NewTable(fixture.TestDB.Ident(), ident.Public, ident.New("all_features"))
	if cfg := s.Targets[tbl]; a.NotNil(cfg) {
		expectedApply := apply.Config{
			CASColumns: []ident.Ident{ident.New("cas0"), ident.New("cas1")},
			Deadlines: map[apply.TargetColumn]time.Duration{
				ident.New("dl0"): time.Hour,
				ident.New("dl1"): time.Minute,
			},
			Exprs: map[apply.TargetColumn]string{
				ident.New("expr0"): "fnv32($0::BYTES)",
				ident.New("expr1"): "true",
			},
			Extras: ident.New("overflow_column"),
			Ignore: map[apply.TargetColumn]bool{
				ident.New("ign0"): true,
				ident.New("ign1"): true,
				// The false value is dropped.
			},
			// SourceName not used; that can be handled by the function.
			SourceNames: map[apply.TargetColumn]apply.SourceColumn{},
		}
		a.Equal(expectedApply, cfg.Config)

		if filter := cfg.Map; a.NotNil(filter) {
			mapped, keep, err := filter(context.Background(), types.Mutation{Data: []byte(`{"hello":"world!"}`)})
			a.NoError(err)
			a.True(keep)
			a.Equal(`{"hello":"world!","msg":"Hello World!","num":42}`, string(mapped.Data))
		}
	}

	tbl = ident.NewTable(fixture.TestDB.Ident(), ident.Public, ident.New("drop_all"))
	if cfg := s.Targets[tbl]; a.NotNil(cfg) {
		if filter := cfg.Map; a.NotNil(filter) {
			_, keep, err := filter(context.Background(), types.Mutation{Data: []byte(`{"hello":"world!"}`)})
			a.NoError(err)
			a.False(keep)
		}
	}
}
