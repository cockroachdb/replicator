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

// Package preflight contains a command to assist with testing database
// connections.
package preflight

import (
	"context"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/stdpool"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// Command returns a command to print the build's bill-of-materials.
func Command() *cobra.Command {
	var staging, target string

	cmd := &cobra.Command{
		Args:  cobra.NoArgs,
		Short: "test connections to database(s)",
		Use:   "preflight",
		// Ignore unknown flags so that you can pass all the arguments in from a start command.
		FParseErrWhitelist: cobra.FParseErrWhitelist{
			UnknownFlags: true,
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if len(target) == 0 && len(staging) == 0 {
				return errors.New("no targetConn or stagingConn specified, no connections to test")
			}
			if len(target) > 0 {
				log.Infof("Testing Target DB (targetConn): %s", target)
				if err := testTargetConnection(ctx, target); err != nil {
					return errors.Wrapf(err, "unable to connect to stagingConn: %s", staging)
				}
			}

			if len(staging) > 0 {
				log.Infof("Testing Staging DB (stagingConn): %s", staging)
				if err := testStagingConnection(ctx, staging); err != nil {
					return errors.Wrapf(err, "unable to connect to stagingConn: %s", staging)
				}
			}
			return nil
		},
	}
	f := cmd.Flags()
	f.StringVar(&staging, "stagingConn", "",
		"the staging CockroachDB cluster's connection string; required if target is other than CRDB")
	f.StringVar(&target, "targetConn", "",
		"the target database's connection string; always required")
	return cmd
}

func testTargetConnection(ctx context.Context, connString string) error {
	log.Infof("testing connecting to the target database: %s", connString)

	pool, cancel, err := stdpool.OpenTarget(
		ctx,
		connString,
		stdpool.WithConnectionLifetime(5*time.Minute),
		stdpool.WithTransactionTimeout(time.Minute),
	)
	if err != nil {
		return err
	}
	defer cancel()

	log.Info("connected to the database")

	switch pool.Product {
	case types.ProductCockroachDB, types.ProductPostgreSQL:
		var result int
		log.Info("Cockroach/Postgres DB detected")
		log.Info("Testing basic query")
		row := pool.DB.QueryRowContext(ctx, "SELECT 1")
		if err := row.Scan(&result); err != nil {
			return err
		}
		if result != 1 {
			return errors.Errorf("SELECT 1 returned %d instead", result)
		}
	case types.ProductMariaDB, types.ProductMySQL:
		log.Info("MySQL DB detected")
		log.Info("Testing basic query")
		var result int
		row := pool.DB.QueryRowContext(ctx, "SELECT 1")
		if err := row.Scan(&result); err != nil {
			return err
		}
		if result != 1 {
			return errors.Errorf("SELECT 1 returned %d instead", result)
		}
	case types.ProductOracle:
		log.Info("Oracle DB detected")
		log.Info("Testing basic query")
		var result int
		row := pool.DB.QueryRowContext(ctx, "SELECT 1 FROM dual")
		if err := row.Scan(&result); err != nil {
			return err
		}
		if result != 1 {
			return errors.Errorf("SELECT 1 from dual; returned %d instead", result)
		}
	default:
		return errors.Errorf("Database type %s not supported.", pool.Product)
	}
	log.Info("Target database test succeeded")
	return nil
}

func testStagingConnection(ctx context.Context, connString string) error {
	log.Infof("connecting to the staging database: %s", connString)

	pool, cancel, err := stdpool.OpenPgxAsStaging(
		ctx,
		connString,
		stdpool.WithConnectionLifetime(5*time.Minute),
		stdpool.WithTransactionTimeout(time.Minute),
	)
	if err != nil {
		return err
	}
	defer cancel()

	log.Info("connected to the database")

	switch pool.Product {
	case types.ProductCockroachDB:
		var result int
		log.Info("Cockroach DB detected")
		log.Info("Testing basic query")
		row := pool.QueryRow(ctx, "SELECT 1")
		if err := row.Scan(&result); err != nil {
			return err
		}
		if result != 1 {
			return errors.Errorf("SELECT 1 returned %d instead", result)
		}
	default:
		return errors.Errorf("Database type %s not supported.", pool.Product)
	}
	log.Info("Staging database test succeeded")
	return nil
}
