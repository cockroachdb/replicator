// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sinktest

import (
	"os"
	"strings"
	"testing"

	log "github.com/sirupsen/logrus"
)

const (
	// IntegrationEnvName is an environment variable that enables
	// integration tests for some databases. We expect this to be of the
	// format "database-v123".
	IntegrationEnvName = "CDC_INTEGRATION"
	// PostgreSQLName must be kept in alignment with the
	// .github/docker-compose.yml file and the integration matrix
	// variable in workflows/tests.yaml.
	PostgreSQLName = "postgresql"
)

// IntegrationMain runs the tests if the value of IntegrationEnvName
// equals the given string, or starts with the given string, followed by
// a hyphen. This method calls os.Exit() and therefore never returns.
func IntegrationMain(m *testing.M, db string) {
	found := os.Getenv(IntegrationEnvName)
	if found == db || strings.HasPrefix(found, db+"-") {
		os.Exit(m.Run())
	}
	log.Infof("skipping %s integration tests: %s=%q", db, IntegrationEnvName, found)
	os.Exit(0)
}
