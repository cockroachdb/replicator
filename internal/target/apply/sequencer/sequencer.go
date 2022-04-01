// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package sequencer applies mutation sequentially, in a transactional consistent order.

package sequencer

import (
	"context"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/jackc/pgtype/pgxtype"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// A Sequencer uses the given stagers and appliers to drain mutations from the
// staging area and apply them to the target tables.
// It keeps track of timestamps, on a schema basis, to insure that mutations
// are applied to satisty a transactional consistent order.
type Sequencer struct {
	appliers   types.Appliers
	stagers    types.Stagers
	timeKeeper types.TimeKeeper
}

// Target specifies the tables destinations of the mutations, as well as
// additional settings for Compare and Set or Deadline operation modes.
type Target struct {
	Tables         []ident.Table
	CasColumns     []ident.Ident
	Deadlines      types.Deadlines
	CheckTimestamp bool
}

// Creates a new Sequencer.
func New(a types.Appliers, s types.Stagers, t types.TimeKeeper) Sequencer {
	return Sequencer{
		appliers:   a,
		stagers:    s,
		timeKeeper: t,
	}
}

// Apply applies the mutations associated with the target.
// Upserts are applied first, in the order specified in the Target array.
// Deletes are applied in reverse order.
func (s *Sequencer) Apply(
	ctx context.Context, tx pgxtype.Querier, target Target, txTime hlc.Time,
) error {

	schemaStartTimes := make(map[ident.Schema]hlc.Time, len(target.Tables))
	for _, tbl := range target.Tables {
		schema := tbl.AsSchema()
		if _, found := schemaStartTimes[schema]; found {
			continue
		}
		prev, err := s.timeKeeper.Put(ctx, tx, tbl.AsSchema(), txTime)
		if err != nil {
			return err
		}
		if target.CheckTimestamp && hlc.Compare(txTime, prev) < 0 {
			return errors.Errorf(
				"resolved timestamp went backwards: received %s had %s",
				txTime, prev)
		}
		schemaStartTimes[schema] = prev
		log.Tracef("committing %s %s -> %s", schema, prev, txTime)
	}
	deletes := make([][]types.Mutation, 0, len(target.Tables))
	appliers := make([]types.Applier, 0, len(target.Tables))
	for _, tbl := range target.Tables {
		prev := schemaStartTimes[tbl.AsSchema()]
		stage, err := s.stagers.Get(ctx, tbl)
		if err != nil {
			return err
		}
		muts, err := stage.Drain(ctx, tx, prev, txTime)
		if err != nil {
			return err
		}
		dmuts := make([]types.Mutation, 0, len(muts))
		umuts := make([]types.Mutation, 0, len(muts))

		for _, m := range muts {
			if m.IsDelete() {
				dmuts = append(dmuts, m)
			} else {
				umuts = append(umuts, m)
			}
		}

		app, err := s.appliers.Get(ctx, tbl, target.CasColumns, target.Deadlines)
		deletes = append(deletes, dmuts)
		appliers = append(appliers, app)
		if err != nil {
			return err
		}
		if err := app.Apply(ctx, tx, umuts); err != nil {
			return err
		}
	}
	for i := len(deletes) - 1; i >= 0; i-- {
		dmuts := deletes[i]
		if err := appliers[i].Apply(ctx, tx, dmuts); err != nil {
			return err
		}
	}
	return nil
}
