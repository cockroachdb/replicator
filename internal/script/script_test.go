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

package script

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/replicator/internal/sinktest"
	"github.com/cockroachdb/replicator/internal/sinktest/all"
	"github.com/cockroachdb/replicator/internal/target/apply"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/applycfg"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/merge"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mapOptions struct {
	data map[string]string
}

func (o *mapOptions) Set(k, v string) error {
	if o.data == nil {
		o.data = map[string]string{k: v}
	} else {
		o.data[k] = v
	}
	return nil
}

func TestScript(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)

	fixture, err := all.NewFixture(t)
	r.NoError(err)

	ctx := fixture.Context
	schema := fixture.TargetSchema.Schema()

	size := 2048
	switch fixture.TargetPool.Product {
	case types.ProductMariaDB, types.ProductMySQL:
		size = 512
	default:
		// nothing to do.
	}

	// Create tables that will be referenced by the user-script.
	_, err = fixture.TargetPool.ExecContext(ctx,
		fmt.Sprintf("CREATE TABLE %s.all_features(msg VARCHAR(%d) PRIMARY KEY)", schema, size))
	r.NoError(err)
	_, err = fixture.TargetPool.ExecContext(ctx,
		fmt.Sprintf("CREATE TABLE %s.delete_swap(pk0 INT, pk1 INT, PRIMARY KEY (pk0, pk1))", schema))
	r.NoError(err)
	_, err = fixture.TargetPool.ExecContext(ctx,
		fmt.Sprintf("CREATE TABLE %s.soft_deletes(pk INT PRIMARY KEY, val INT NOT NULL, is_deleted INT DEFAULT 0 NOT NULL)", schema))
	r.NoError(err)
	_, err = fixture.TargetPool.ExecContext(ctx,
		fmt.Sprintf("CREATE TABLE %s.sql_test(pk INT PRIMARY KEY, val INT NOT NULL)", schema))
	r.NoError(err)
	_, err = fixture.TargetPool.ExecContext(ctx,
		fmt.Sprintf("CREATE TABLE %s.table1(msg VARCHAR(%d) PRIMARY KEY)", schema, size))
	r.NoError(err)
	_, err = fixture.TargetPool.ExecContext(ctx,
		fmt.Sprintf("CREATE TABLE %s.table2(idx INT PRIMARY KEY)", schema))
	r.NoError(err)
	_, err = fixture.TargetPool.ExecContext(ctx,
		fmt.Sprintf(`
CREATE TABLE %s.skewed_merge_times(
 pk INT PRIMARY KEY,
 epoch INT DEFAULT 0 NOT NULL,
 updated_at TIMESTAMP NOT NULL
)`, schema))
	r.NoError(err)

	r.NoError(fixture.Watcher.Refresh(ctx, fixture.TargetPool))
	var opts mapOptions

	cfg := &Config{
		Options:        &opts,
		UserScriptPath: "./testdata/main.ts",
	}
	loader, err := ProvideLoader(ctx, fixture.Configs, cfg, fixture.Diagnostics)
	r.NoError(err)

	s, err := loader.Bind(ctx, fixture.TargetSchema, fixture.ApplyAcceptor, fixture.Watchers)
	r.NoError(err)

	r.NoError(s.watcher.Refresh(ctx, fixture.TargetPool))
	a.Equal(3, s.Sources.Len())
	a.Equal(10, s.Targets.Len())
	a.Equal(map[string]string{"hello": "world"}, opts.data)

	tbl1 := ident.NewTable(schema, ident.New("table1"))
	tbl2 := ident.NewTable(schema, ident.New("table2"))

	if cfg := s.Sources.GetZero(ident.New("expander")); a.NotNil(cfg) {
		mut := types.Mutation{
			Before: []byte(`{"before":true}`),
			Data:   []byte(`{"msg":true,"idx":0}`),
			Key:    []byte(`["key"]`),
		}
		AddMeta("test", tbl1, &mut)
		// Check that deletes can be sent to both tables.
		if a.NotNil(cfg.DeletesTo) {
			mutNoData := mut
			mutNoData.Data = nil
			a.True(mutNoData.IsDelete())
			if mapped, err := cfg.DeletesTo(ctx, tbl1, mutNoData); a.NoError(err) {
				a.Equal(2, mapped.Len())
				// This should pass the original document through.
				if found := mapped.GetZero(tbl1); a.Len(found, 1) {
					a.Equal(mutNoData, found[0])
				}
				// We really just care about the key at this point.
				if found := mapped.GetZero(tbl2); a.Len(found, 1) {
					a.Zero(found[0].Data)
					a.Equal(json.RawMessage(`[42]`), found[0].Key)
				}
			}
		}
		mapped, err := cfg.Dispatch(context.Background(), tbl1, mut)
		if a.NoError(err) && a.NotNil(mapped) {
			if docs := mapped.GetZero(tbl1); a.Len(docs, 1) {
				a.Equal(`{"before":true}`, string(docs[0].Before))
				a.Equal(`{"dest":"table1","msg":true}`, string(docs[0].Data))
				a.Equal(`[true]`, string(docs[0].Key))
			}

			if docs := mapped.GetZero(tbl2); a.Len(docs, 2) {
				a.Equal(`{"before":true}`, string(docs[0].Before))
				a.Equal(`{"dest":"table2","idx":0,"msg":true}`, string(docs[0].Data))
				a.Equal(`[0]`, string(docs[0].Key))

				a.Equal(`{"before":true}`, string(docs[1].Before))
				a.Equal(`{"dest":"table2","idx":1,"msg":true}`, string(docs[1].Data))
				a.Equal(`[1]`, string(docs[1].Key))
			}
		}
	}

	if cfg := s.Sources.GetZero(ident.New("passthrough")); a.NotNil(cfg) {
		mut := types.Mutation{Data: []byte(`{"passthrough":true}`)}
		someTable := ident.NewTable(schema, ident.New("some_table"))
		if a.NotNil(cfg.DeletesTo) {
			mutNoData := mut
			mutNoData.Data = nil
			if mapped, err := cfg.DeletesTo(ctx, someTable, mut); a.NoError(err) {
				if found := mapped.GetZero(someTable); a.Len(found, 1) {
					a.Equal(mutNoData, found[0])
				}
			}
		}
		mapped, err := cfg.Dispatch(context.Background(), someTable, mut)
		if a.NoError(err) && a.NotNil(mapped) {
			expanded := mapped.GetZero(someTable)
			if a.Len(expanded, 1) {
				a.Equal(mut, expanded[0])
			}
		}
	}

	if cfg := s.Sources.GetZero(ident.New("recursive")); a.NotNil(cfg) {
		a.True(cfg.Recurse)
	}

	table := ident.NewTable(schema, ident.New("all_features"))
	if cfg := s.Targets.GetZero(table); a.NotNil(cfg) {
		expectedApply := applycfg.Config{
			CASColumns: []ident.Ident{ident.New("cas0"), ident.New("cas1")},
			Deadlines: ident.MapOf[time.Duration](
				ident.New("dl0"), time.Hour,
				ident.New("dl1"), time.Minute,
			),
			Exprs: ident.MapOf[string](
				ident.New("expr0"), "fnv32($0::BYTES)",
				ident.New("expr1"), "Hello Library!",
			),
			Extras: ident.New("overflow_column"),
			Ignore: ident.MapOf[bool](
				ident.New("ign0"), true,
				ident.New("ign1"), true,
				// The false value is dropped.
			),
			// SourceName not used; that can be handled by the function.
			SourceNames: &ident.Map[applycfg.SourceColumn]{},
			RowLimit:    99,
		}
		a.True(expectedApply.Equal(&cfg.Config))

		if filter := cfg.Map; a.NotNil(filter) {
			mapped, keep, err := filter(context.Background(), types.Mutation{Data: []byte(`{"hello":"world!"}`)})
			a.NoError(err)
			a.True(keep)
			a.Equal(`{"hello":"world!","msg":"Hello World!","num":42}`, string(mapped.Data))
		}

		if merger := cfg.Merger; a.NotNil(merger) {
			result, err := merger.Merge(context.Background(), &merge.Conflict{
				Before:   merge.NewBagOf(nil, nil, "val", 1),
				Proposed: merge.NewBagOf(nil, nil, "val", 3),
				Target:   merge.NewBagOf(nil, nil, "val", 40),
			})
			a.NoError(err)
			if a.NotNil(result.Apply) {
				if v, ok := result.Apply.Get(ident.New("val")); a.True(ok) {
					a.Equal(int64(42), v)
				}
			}
		}
	}

	// Unconditionally ignore deletions.
	table = ident.NewTable(schema, ident.New("delete_elide"))
	if cfg := s.Targets.GetZero(table); a.NotNil(cfg) {
		if filter := cfg.DeleteKey; a.NotNil(filter) {
			_, keep, err := filter(context.Background(), types.Mutation{Key: []byte(`[ 1, 2 ]`)})
			a.NoError(err)
			a.False(keep)
		}
	}

	table = ident.NewTable(schema, ident.New("delete_swap"))
	if cfg := s.Targets.GetZero(table); a.NotNil(cfg) {
		if filter := cfg.DeleteKey; a.NotNil(filter) {
			mut, keep, err := filter(context.Background(), types.Mutation{Key: []byte(`[ 1, 2 ]`)})
			a.NoError(err)
			a.True(keep)
			a.Equal(json.RawMessage(`["2","1"]`), mut.Key)
		}
	}

	// A map function that unconditionally filters all mutations.
	table = ident.NewTable(schema, ident.New("drop_all"))
	if cfg := s.Targets.GetZero(table); a.NotNil(cfg) {
		if filter := cfg.Map; a.NotNil(filter) {
			_, keep, err := filter(context.Background(), types.Mutation{Data: []byte(`{"hello":"world!"}`)})
			a.NoError(err)
			a.False(keep)
		}
	}

	// A merge function that sends all conflicts to a DLQ.
	table = ident.NewTable(schema, ident.New("merge_dlq_all"))
	if cfg := s.Targets.GetZero(table); a.NotNil(cfg) {
		if merger := cfg.Merger; a.NotNil(merger) {
			result, err := merger.Merge(context.Background(), &merge.Conflict{
				Before:   merge.NewBagOf(nil, nil, "val", 1),
				Proposed: merge.NewBagOf(nil, nil, "val", 2),
				Target:   merge.NewBagOf(nil, nil, "val", 0),
			})
			a.NoError(err)
			a.Equal(&merge.Resolution{DLQ: "dead"}, result)
		}
	}

	// A merge function that drops all conflicts.
	table = ident.NewTable(schema, ident.New("merge_drop_all"))
	if cfg := s.Targets.GetZero(table); a.NotNil(cfg) {
		if merger := cfg.Merger; a.NotNil(merger) {
			result, err := merger.Merge(context.Background(), &merge.Conflict{
				Before:   merge.NewBagOf(nil, nil, "val", 1),
				Proposed: merge.NewBagOf(nil, nil, "val", 2),
				Target:   merge.NewBagOf(nil, nil, "val", 0),
			})
			a.NoError(err)
			a.Equal(&merge.Resolution{Drop: true}, result)
		}
	}

	// A merge function that sends to a DLQ on conflict.
	table = ident.NewTable(schema, ident.New("merge_or_dlq"))
	if cfg := s.Targets.GetZero(table); a.NotNil(cfg) {
		if merger := cfg.Merger; a.NotNil(merger) {
			result, err := merger.Merge(context.Background(), &merge.Conflict{
				Before:   merge.NewBagOf(nil, nil, "val", 1),
				Proposed: merge.NewBagOf(nil, nil, "val", 2),
				Target:   merge.NewBagOf(nil, nil, "val", 0),
			})
			a.NoError(err)
			a.Equal(&merge.Resolution{DLQ: "dead"}, result)
		}
	}

	t.Run("soft_delete", func(t *testing.T) {
		r := require.New(t)
		table = ident.NewTable(schema, ident.New("soft_deletes"))

		// Seed the table with a row we want to soft-delete
		r.NoError(fixture.ApplyAcceptor.AcceptTableBatch(ctx, sinktest.TableBatchOf(
			table, hlc.New(100, 1), []types.Mutation{
				{
					Data: json.RawMessage(`{"pk":1,"val":2}`),
					Key:  json.RawMessage(`[1]`),
				},
				{
					Data: json.RawMessage(`{"pk":3,"val":4}`),
					Key:  json.RawMessage(`[3]`),
				},
			},
		), &types.AcceptOptions{}))

		cfg := s.Targets.GetZero(table)
		r.NotNil(cfg)
		acceptor := cfg.UserAcceptor
		r.IsType(new(applier), acceptor)

		// Send some deletes using a diff style. This lets us provide
		// the script with the previous values to copy forward. Allowing
		// the script to be able to load individual records using only
		// the PK would improve the ergonomics.
		//
		// https://github.com/cockroachdb/replicator/issues/705
		tx, err := fixture.TargetPool.BeginTx(ctx, nil)
		r.NoError(err)
		defer func() { _ = tx.Rollback() }()

		r.NoError(acceptor.AcceptTableBatch(ctx, sinktest.TableBatchOf(
			table, hlc.New(100, 1), []types.Mutation{
				{
					Before: json.RawMessage(`{"pk":1,"val":2}`),
					Key:    json.RawMessage(`[1]`),
				},
				{
					Before: json.RawMessage(`{"pk":3,"val":4}`),
					Key:    json.RawMessage(`[3]`),
				},
			},
		), &types.AcceptOptions{TargetQuerier: tx}))
		r.NoError(tx.Commit())

		// Fix case for e.g. Oracle.
		table, _ = fixture.Watcher.Get().OriginalName(table)
		// Verify execution.
		var count int
		r.NoError(fixture.TargetPool.QueryRow(
			fmt.Sprintf("SELECT count(*) FROM %s WHERE is_deleted = 1", table),
		).Scan(&count))
		r.Equal(2, count)
	})

	// The SQL in the test script is opinionated.
	t.Run("sql_test", func(t *testing.T) {
		if fixture.TargetPool.Product != types.ProductCockroachDB {
			t.Skip("user script has opinionated SQL")
		}
		r := require.New(t)
		table = ident.NewTable(schema, ident.New("sql_test"))
		cfg := s.Targets.GetZero(table)
		r.NotNil(cfg)
		acceptor := cfg.UserAcceptor
		r.IsType(new(applier), acceptor)
		r.NoError(acceptor.AcceptTableBatch(ctx, sinktest.TableBatchOf(table, hlc.Zero(), []types.Mutation{
			{
				Data: json.RawMessage(`{"pk":0,"val":0}`),
				Key:  json.RawMessage(`[0]`),
			},
			{
				Data: json.RawMessage(`{"pk":1,"val":1}`),
				Key:  json.RawMessage(`[1]`),
			},
			{
				Data: json.RawMessage(`{"pk":2,"val":2}`),
				Key:  json.RawMessage(`[2]`),
			}}),
			&types.AcceptOptions{TargetQuerier: fixture.TargetPool},
		))
		rows, err := fixture.TargetPool.QueryContext(ctx,
			fmt.Sprintf("SELECT pk, val FROM %s ORDER BY pk", table))
		r.NoError(err)
		ct := 0
		for rows.Next() {
			ct++
			var pk, val int
			r.NoError(rows.Scan(&pk, &val))
			r.Equal(pk, -1*val)
		}
		r.NoError(rows.Err())
		r.Equal(3, ct)

		// Check deletion
		r.NoError(acceptor.AcceptTableBatch(ctx, sinktest.TableBatchOf(table, hlc.Zero(), []types.Mutation{
			{
				Key: json.RawMessage(`[0]`),
			},
			{
				Key: json.RawMessage(`[1]`),
			},
			{
				Key: json.RawMessage(`[2]`),
			}}),
			&types.AcceptOptions{TargetQuerier: fixture.TargetPool},
		))
		r.NoError(fixture.TargetPool.QueryRow(fmt.Sprintf(
			"SELECT count(*) FROM %s", table)).Scan(&ct))
		r.Equal(0, ct)
	})

	t.Run("skewed_merge_times", func(t *testing.T) {
		if !apply.IsMergeSupported(fixture.TargetPool.Product) {
			t.Skipf("merge operation not implemented for product %s", fixture.TargetPool.Product)
		}
		r := require.New(t)
		table = ident.NewTable(schema, ident.New("skewed_merge_times"))
		cfg := s.Targets.GetZero(table)
		r.NotNil(cfg)
		acceptor := cfg.UserAcceptor
		r.IsType(new(applier), acceptor)
		r.NotNil(cfg.Merger)

		// We're going to make the updated_at column count backwards.
		now := time.Now().UTC()
		for i := 10; i > 0; i-- {
			r.NoError(acceptor.AcceptTableBatch(ctx, sinktest.TableBatchOf(table, hlc.Zero(), []types.Mutation{
				{
					Data: json.RawMessage(fmt.Sprintf(
						`{"pk":0, "updated_at":%q}`,
						now.Add(time.Duration(i)*time.Second).Format(time.RFC3339Nano))),
					Key: json.RawMessage(`[0]`),
				},
			}),
				&types.AcceptOptions{TargetQuerier: fixture.TargetPool},
			))
		}
	})
}

// This test ensures that the old name continues to function.
func TestLegacyImport(t *testing.T) {
	r := require.New(t)

	fixture, err := all.NewFixture(t)
	r.NoError(err)

	ctx := fixture.Context
	var opts mapOptions
	cfg := &Config{
		Options:        &opts,
		UserScriptPath: "./testdata/legacy_import.ts",
	}
	_, err = ProvideLoader(ctx, fixture.Configs, cfg, fixture.Diagnostics)
	r.NoError(err)
	r.Equal(map[string]string{"old": "world"}, opts.data)
}
