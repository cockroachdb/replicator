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

package votr

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/retry"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

var (
	ballots    = ident.New("ballots")
	candidates = ident.New("candidates")
	totals     = ident.New("totals")

	names = [...]string{
		"Alice", "Bob", "Carol", "David", "Eve", "Frank", "Gil",
		"Hillary", "Indira", "Jill", "Kyle", "Louis", "Mike", "Nancy",
		"Oscar", "Paul", "Queen", "Romeo", "Sierra", "Toni", "Ursula",
		"Vik", "Walter", "Xerxes", "Yolanda", "Zola",
	}
	connectors = [...]string{"le", "the"}
	epithets   = [...]string{
		"Awesome", "Boor", "Concerned", "Dependable", "Elated", "Fancy",
		"Grouch", "Hapless", "Indecisive", "Joyful", "Kleptocrat",
		"Lesser", "Mannered", "Nice", "Opulent", "Purposeful", "Quiet",
		"Remote", "Sulky", "Truthful", "Unfortunate", "Victorious",
		"Wastrel", "XIVth", "Yankee", "Zoologist",
	}
	moods = [...]string{
		"Active", "Bad", "Cheerful", "Down", "Elated", "Frightened",
		"Good", "Happy", "Introspective", "Justified", "Kind", "Liked",
		"Mad", "Naughty", "Open", "Puzzled", "Questioning", "Romantic",
		"Sleepy", "Trusting", "Watchful", "XOXO", "Zen",
	}
)

const (
	// ballots are append-only.
	ballotsSchema = `CREATE TABLE IF NOT EXISTS %[1]s (
candidate UUID NOT NULL REFERENCES %[2]s ON DELETE CASCADE,
ballot UUID NOT NULL DEFAULT gen_random_uuid(),
whence JSONB NOT NULL
DEFAULT jsonb_build_object(%[3]d, cluster_logical_timestamp()::string)
ON UPDATE jsonb_build_object(%[3]d, cluster_logical_timestamp()::string),
src INT NOT NULL DEFAULT %[3]d ON UPDATE %[3]d,
xyzzy INT NOT NULL DEFAULT 0, -- Hack to force a conflict, until CAS mode has "always" mode.
PRIMARY KEY (candidate, ballot)
)`

	// candidates might be updated occasionally in a last-one-wins manner.
	candidatesSchema = `CREATE TABLE IF NOT EXISTS %[1]s (
candidate UUID PRIMARY KEY,
whence JSONB NOT NULL
DEFAULT jsonb_build_object(%[2]d, cluster_logical_timestamp()::string)
ON UPDATE jsonb_build_object(%[2]d, cluster_logical_timestamp()::string),
src INT NOT NULL DEFAULT %[2]d ON UPDATE %[2]d,
name TEXT NOT NULL,
mood TEXT NOT NULL,
xyzzy INT NOT NULL DEFAULT 0  -- Hack to force a conflict, until CAS mode has "always" mode.
)`

	// totals will show a high-conflict table with custom merge logic.
	totalsSchema = `CREATE TABLE IF NOT EXISTS %[1]s (
candidate UUID PRIMARY KEY REFERENCES %[2]s ON DELETE CASCADE,
whence JSONB NOT NULL
DEFAULT jsonb_build_object(%[3]d, cluster_logical_timestamp()::string)
ON UPDATE jsonb_build_object(%[3]d, cluster_logical_timestamp()::string),
src INT NOT NULL DEFAULT %[3]d ON UPDATE %[3]d,
total INT NOT NULL DEFAULT 0,
xyzzy INT NOT NULL DEFAULT 0  -- Hack to force a conflict, until CAS mode has "always" mode.
)`
)

type schema struct {
	ballots    ident.Table
	candidates ident.Table
	enclosing  ident.Ident
	totals     ident.Table

	candidateIds map[uuid.UUID]struct{}
	db           *types.SourcePool
	region       region
}

func newSchema(db *types.SourcePool, enclosing ident.Ident, r region) *schema {
	enclosing = ident.New(enclosing.Raw() + "_" + r.String())
	s := ident.MustSchema(enclosing, ident.Public)
	return &schema{
		ballots:      ident.NewTable(s, ballots),
		candidateIds: make(map[uuid.UUID]struct{}),
		candidates:   ident.NewTable(s, candidates),
		db:           db,
		enclosing:    enclosing,
		region:       r,
		totals:       ident.NewTable(s, totals),
	}
}

func (s *schema) create(ctx context.Context) error {
	return retry.Retry(ctx, func(ctx context.Context) error {
		if _, err := s.db.ExecContext(ctx, fmt.Sprintf(
			`DROP DATABASE IF EXISTS %s CASCADE`, s.enclosing)); err != nil {
			return errors.WithStack(err)
		}

		if _, err := s.db.ExecContext(ctx, fmt.Sprintf(
			`CREATE DATABASE %s `, s.enclosing)); err != nil {
			return errors.WithStack(err)
		}

		if _, err := s.db.ExecContext(ctx, fmt.Sprintf(
			candidatesSchema, s.candidates, s.region,
		)); err != nil {
			return errors.WithStack(err)
		}

		if _, err := s.db.ExecContext(ctx, fmt.Sprintf(
			ballotsSchema, s.ballots, s.candidates, s.region,
		)); err != nil {
			return errors.WithStack(err)
		}

		if _, err := s.db.ExecContext(ctx, fmt.Sprintf(
			totalsSchema, s.totals, s.candidates, s.region,
		)); err != nil {
			return errors.WithStack(err)
		}
		return nil
	})
}

// doStuff selects a random selection of candidates, distributes the
// requested number of votes across them, and inserts the ballots.
func (s *schema) doStuff(ctx context.Context, votes int) error {
	if votes <= 0 {
		return nil
	}
	numCandidates := rand.Intn(votes) + 1 // Intn [0,n)

	winners := make([]uuid.UUID, 0, numCandidates)
	// Iteration over a map is random enough for our purposes.
	for id := range s.candidateIds {
		winners = append(winners, id)
		if len(winners) == numCandidates {
			break
		}
	}

	voteAllocation := make(map[uuid.UUID]int)
	for i := 0; i < votes; i++ {
		winnerIdx := i % len(winners)
		voteAllocation[winners[winnerIdx]]++
	}

	ballotQ := fmt.Sprintf(`INSERT INTO %s (candidate)
SELECT candidate FROM
(SELECT $1::UUID candidate, generate_series(1, $2))`, s.ballots)
	totalQ := fmt.Sprintf(`INSERT INTO %s AS tbl (candidate, total)
VALUES ($1, $2)
ON CONFLICT(candidate)
DO UPDATE SET total=tbl.total+excluded.total`, s.totals)

	return retry.Retry(ctx, func(ctx context.Context) error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return errors.WithStack(err)
		}
		defer func() { _ = tx.Rollback() }()

		for candidate, count := range voteAllocation {
			if _, err := tx.ExecContext(ctx, totalQ, candidate, count); err != nil {
				return errors.WithStack(err)
			}
			if _, err := tx.ExecContext(ctx, ballotQ, candidate, count); err != nil {
				return errors.WithStack(err)
			}
		}
		if err := errors.WithStack(tx.Commit()); err != nil {
			return err
		}
		return nil
	})
}

func (s *schema) ensureCandidates(ctx context.Context, count int) error {
	seed := int64(0)
	rnd := rand.New(rand.NewSource(seed))

	nextMood := func() string {
		return moods[rnd.Intn(len(moods))]
	}
	nextName := func(deconflict int) string {
		return fmt.Sprintf("%s %s %s (%d)",
			names[rnd.Intn(len(names))],
			connectors[rnd.Intn(len(connectors))],
			epithets[rnd.Intn(len(epithets))],
			deconflict)
	}

	// Rows are inserted with deterministic ids.
	q := fmt.Sprintf(`UPSERT INTO %s (candidate, name, mood)
VALUES (uuid_generate_v5('455E049E-54B6-41C9-BBCE-1587CC394851', $1), $1, $2)
RETURNING candidate`, s.candidates)

	for i := 0; i < count; i++ {
		name := nextName(i)
		mood := nextMood()
		if err := retry.Retry(ctx, func(ctx context.Context) error {
			var id uuid.UUID
			if err := s.db.QueryRowContext(ctx, q, name, mood).Scan(&id); err != nil {
				return errors.WithStack(err)
			}
			s.candidateIds[id] = struct{}{}
			return nil
		}); err != nil {
			return err
		}
	}

	return nil
}

func (s *schema) sumTotal(ctx context.Context) (int, error) {
	var total sql.NullInt64
	err := s.db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT sum(total) FROM %s", s.totals),
	).Scan(&total)
	return int(total.Int64), errors.Wrapf(err, "sum total in region %s", s.region)
}

func (s *schema) validate(ctx context.Context, aost bool) ([]string, error) {
	asOf := ""
	if aost {
		asOf = "AS OF SYSTEM TIME follower_read_timestamp()"
	}

	q := fmt.Sprintf(`
  WITH counted AS (SELECT candidate, count(*) AS count FROM %s GROUP BY candidate),
       verify AS (
               SELECT candidate,
                      IFNULL(counted.count, 0) expected,
                      IFNULL(totals.total, 0) actual
                 FROM counted FULL JOIN %s USING (candidate)
              )
SELECT candidate, expected, actual, name
  FROM verify
  JOIN %s USING (candidate)
  %s
 WHERE expected != actual`, s.ballots, s.totals, s.candidates, asOf)

	var ret []string
	err := retry.Retry(ctx, func(ctx context.Context) error {
		ret = nil // Reset if looping.

		rows, err := s.db.QueryContext(ctx, q)
		if err != nil {
			return errors.WithStack(err)
		}
		defer func() { _ = rows.Close() }()

		for rows.Next() {
			var id uuid.UUID
			var expected, actual int
			var name string
			if err := rows.Scan(&id, &expected, &actual, &name); err != nil {
				return errors.WithStack(err)
			}
			ret = append(ret, fmt.Sprintf("%s: expected %d had %d (%s)",
				id, expected, actual, name))
		}
		// Final error check.
		return errors.WithStack(rows.Err())
	})
	return ret, err
}
