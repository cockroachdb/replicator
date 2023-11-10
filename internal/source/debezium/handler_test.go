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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/sinktest/all"
	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sendBatch(ctx context.Context, h *Handler, file string) error {
	buff, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	h.processImmediate(ctx, buff)
	if err != nil {
		return err
	}
	return nil
}
func TestBatch(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)
	// Create a target database.
	fixture, err := all.NewFixture(t)
	r.NoError(err)
	ctx := fixture.Context
	dbName := fixture.TargetSchema.Schema()
	pool := fixture.TargetPool

	// Create the schema in the target database
	tgt := ident.NewTable(dbName, ident.New("t"))
	crdbSchema := fmt.Sprintf(`CREATE TABLE %s (i INT, j int, k int, primary key (i,j))`, tgt)
	_, err = pool.ExecContext(ctx, crdbSchema)
	r.NoError(err)

	cfg := &logical.BaseConfig{
		ApplyTimeout:   time.Second, // Increase to make using the debugger easier.
		Immediate:      true,
		RetryDelay:     time.Millisecond,
		StagingConn:    fixture.StagingPool.ConnectionString,
		StagingSchema:  fixture.StagingDB.Schema(),
		StandbyTimeout: 5 * time.Millisecond,
		TargetConn:     pool.ConnectionString,
	}
	factory, err := logical.NewFactoryForTests(ctx, cfg)
	r.NoError(err)
	batcher, err := factory.Immediate(dbName)
	r.NoError(err)
	h := &Handler{
		Batcher: batcher,
		Stop:    ctx,
		Config:  &Config{},
	}

	h.Config.TargetSchema = dbName

	tests := []struct {
		name     string
		query    string
		expected int
	}{
		{"insert",
			fmt.Sprintf("SELECT count(*) FROM %s", tgt),
			10,
		},
		{"update",
			fmt.Sprintf("SELECT count(*) FROM %s where k=2", tgt),
			10,
		},
		{"delete",
			fmt.Sprintf("SELECT count(*) FROM %s", tgt),
			0,
		},
	}
	// Do not run in parallel. Tests are not independent.
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fmt.Println("running " + tt.name)
			go func(ctx context.Context) {
				sendBatch(ctx, h, "./testdata/"+tt.name+"_batch.json")
			}(ctx)
			var count int
			for {
				err := pool.QueryRowContext(ctx, tt.query).Scan(&count)
				r.NoError(err)
				if count == tt.expected {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			a.Equal(tt.expected, count)
		})
	}
	ctx.Stop(time.Second)
	a.NoError(ctx.Wait())
}
