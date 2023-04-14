// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package fan

import (
	"testing"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/batches"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchCoalesce(t *testing.T) {
	stampedBackSanityCheck = true

	m := types.Mutation{
		Data: []byte("SOME DATA"),
		Key:  []byte("SOME KEY"),
		Time: hlc.From(time.Now()),
		Meta: nil,
	}

	t.Run("append-until-full", func(t *testing.T) {
		a := assert.New(t)

		batch := newBatch(nil, m)
		for {
			tail := newBatch(nil, m)

			leftover := batch.Coalesce(tail)
			if leftover != nil {
				break
			}
		}

		muts, weight := batch.Pick()

		a.Len(muts, batches.Size())
		a.Equal(int64(batches.Size()*(len(m.Data)+len(m.Key))), weight)
	})

	// Verify that a picked batch cannot coalesce or be coalesced.
	t.Run("coalesce-picked", func(t *testing.T) {
		a := assert.New(t)

		b1 := newBatch(nil, m)
		b2 := newBatch(nil, m)

		b1.Pick()
		a.Same(b2, b1.Coalesce(b2))
		a.Same(b1, b2.Coalesce(b1))
	})

	t.Run("partial-coalesce", func(t *testing.T) {
		r := require.New(t)

		max := 3 * batches.Size() / 4

		b1 := newBatch(nil, m)
		b2 := newBatch(nil, m)
		for i := 0; i < max-1; i++ {
			r.Nil(b1.Coalesce(newBatch(nil, m)))
			r.Nil(b2.Coalesce(newBatch(nil, m)))
		}

		r.Len(b1.mu.muts, max)
		r.Len(b2.mu.muts, max)

		rem := b1.Coalesce(b2)
		r.Len(b1.mu.muts, batches.Size())
		r.Equal(int64(batches.Size()*(len(m.Data)+len(m.Key))), b1.Weight())

		r.Same(b2, rem)
		r.Len(b2.mu.muts, batches.Size()/2)
		r.Equal(int64(batches.Size()/2*(len(m.Data)+len(m.Key))), b2.Weight())
	})
}
