// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package fslogical

import (
	"os"

	"github.com/cockroachdb/cdc-sink/internal/source/logical"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

type loopConfig struct {
	BackfillBatch     int
	SourceCollection  string
	TargetTable       ident.Table
	UpdatedAtProperty ident.Ident
}

// Config adds dialect-specific configuration to the core logical loop.
type Config struct {
	logical.Config
	// The number of documents to load at once during a backfill operation.
	BackfillBatchSize int
	// A JSON service-account key for the Firestore API.
	CredentialsFile string
	// The Firebase project id. Usually inferred from the credentials.
	ProjectID         string
	SourceCollections []string
	TargetTables      []ident.Table
	// The name of a document property used for high-water marks.
	UpdatedAtProperty ident.Ident

	updatedAtTemp string
	tablesTemp    []string
}

// Bind adds flags to the pflag.FlagSet to populate the Config.
func (c *Config) Bind(f *pflag.FlagSet) {
	c.Config.Bind(f)
	f.IntVar(&c.BackfillBatchSize, "backfillBatchSize", 100,
		"the number of documents to load when backfilling")
	f.StringVar(&c.CredentialsFile, "credentials", "",
		"a file containing JSON service credentials.")
	f.StringVar(&c.ProjectID, "projectID", "",
		"override the project id contained in the credentials file")
	f.StringSliceVarP(&c.SourceCollections, "collection", "c", nil,
		"one or more source document collections")
	f.StringSliceVarP(&c.tablesTemp, "table", "t", nil,
		"one or more destination table names")
	f.StringVar(&c.updatedAtTemp, "updatedAt", "updatedAt",
		"the name of a document property used for high-water marks")
}

// Preflight adds additional checks to the base logical.Config.
func (c *Config) Preflight() error {
	if err := c.Config.Preflight(); err != nil {
		return err
	}

	if c.BackfillBatchSize < 1 {
		return errors.New("backfill batch size must be >= 1")
	}

	// Only require credentials if there's no emulator.
	if os.Getenv(emulatorEnv) == "" {
		if c.CredentialsFile == "" {
			return errors.New("no credentials file specified")
		}
		if _, err := os.Stat(c.CredentialsFile); err != nil {
			return errors.Errorf("could not stat %s", c.CredentialsFile)
		}
	}

	// Sanity-check collections vs target tables.
	if len(c.SourceCollections) != len(c.tablesTemp) {
		return errors.Errorf("unequal source collections %d vs target tables %d",
			len(c.SourceCollections), len(c.tablesTemp))
	}
	c.TargetTables = make([]ident.Table, len(c.tablesTemp))
	for i := range c.TargetTables {
		c.TargetTables[i] = ident.NewTable(
			c.TargetDB, ident.Public, ident.New(c.tablesTemp[i]))
	}
	c.tablesTemp = nil

	if c.updatedAtTemp == "" {
		return errors.New("no updated_at property name given")
	}
	c.UpdatedAtProperty = ident.New(c.updatedAtTemp)
	c.updatedAtTemp = ""

	return nil
}
