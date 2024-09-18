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

package all

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
	// KafkaName must be kept in alignment with the
	// .github/docker-compose.yml file and the integration matrix
	// variable in workflows/tests.yaml.
	KafkaName = "kafka"

	// MySQLName must be kept in alignment with the
	// .github/docker-compose.yml file and the integration matrix
	// variable in workflows/tests.yaml.
	MySQLName = "mysql"

	// ObjectStoreName must be kept in alignment with the
	// .github/docker-compose.yml file and the integration matrix
	// variable in workflows/tests.yaml.
	ObjectStoreName = "objstore"

	// PostgreSQLName must be kept in alignment with the
	// .github/docker-compose.yml file and the integration matrix
	// variable in workflows/tests.yaml.
	PostgreSQLName = "postgresql"

	// OracleName must be kept in alignment with the
	// .github/docker-compose.yml file and the integration matrix
	// variable in workflows/tests.yaml.
	OracleName = "oracle"
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
