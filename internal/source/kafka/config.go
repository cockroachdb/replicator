// Copyright 2024 The Cockroach Authors
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

package kafka

import (
	"context"
	"math"
	"net/url"
	"time"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/sequencer"
	"github.com/cockroachdb/cdc-sink/internal/sinkprod"
	"github.com/cockroachdb/cdc-sink/internal/target/dlq"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/cockroachdb/cdc-sink/internal/util/secure"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"golang.org/x/oauth2/clientcredentials"
)

// EagerConfig is a hack to get Wire to move userscript evaluation to
// the beginning of the injector. This allows CLI flags to be set by the
// script.
type EagerConfig Config

// Config contains the configuration necessary for creating a
// replication connection. ServerID and SourceConn are mandatory.
type Config struct {
	DLQ          dlq.Config
	Script       script.Config
	Sequencer    sequencer.Config
	Staging      sinkprod.StagingConfig
	Target       sinkprod.TargetConfig
	TargetSchema ident.Schema
	TLS          secure.Config

	BatchSize        int           // How many messages to accumulate before committing to the target
	Brokers          []string      // The address of the Kafka brokers
	Group            string        // the Kafka consumer group id.
	MaxTimestamp     string        // Only accept messages at or older than this timestamp
	MinTimestamp     string        // Only accept messages at or newer than this timestamp
	ResolvedInterval time.Duration // Minimal duration between resolved timestamps.
	Strategy         string        // Kafka consumer group re-balance strategy
	Topics           []string      // The list of topics that the consumer should use.

	// SASL
	saslClientID     string
	saslClientSecret string
	saslGrantType    string
	saslMechanism    string
	saslScopes       []string
	saslTokenURL     string
	saslUser         string
	saslPassword     string

	// The following are computed.

	// Timestamp range, computed based on minTimestamp and maxTimestamp.
	timeRange hlc.Range

	// The kafka connector configuration.
	saramaConfig *sarama.Config
}

// Bind adds flags to the set. It delegates to the embedded Config.Bind.
func (c *Config) Bind(f *pflag.FlagSet) {
	c.DLQ.Bind(f)
	c.Script.Bind(f)
	c.Sequencer.Bind(f)
	c.Staging.Bind(f)
	c.Target.Bind(f)
	f.Var(ident.NewSchemaFlag(&c.TargetSchema), "targetSchema",
		"the SQL database schema in the target cluster to update")

	f.IntVar(&c.BatchSize, "batchSize", 100, "messages to accumulate before committing to the target")
	f.StringArrayVar(&c.Brokers, "broker", nil, "address of Kafka broker(s)")
	f.StringVar(&c.Group, "group", "", "the Kafka consumer group id")
	f.StringVar(&c.MaxTimestamp, "maxTimestamp", "",
		"only accept messages older than this timestamp; this is an exclusive upper limit")
	f.StringVar(&c.MinTimestamp, "minTimestamp", "",
		"only accept unprocessed messages at or newer than this timestamp; this is an inclusive lower limit")
	f.DurationVar(&c.ResolvedInterval, "resolvedInterval", 5*time.Second, `interval between two resolved timestamps.
Only used when minTimestamp is specified.
It serves as a hint to seek the offset of a resolved timestamp message
that is strictly less than the minTimestamp in the Kafka feed.
Note:
The optimal value for resolvedInterval is the same as the resolved
interval specified in the CREATE CHANGEFEED command.
The resolved messages will not be emitted more frequently than
the configured min_checkpoint_frequency specified in CREATE CHANGEFEED
command (but may be emitted less frequently).
Please see the CREATE CHANGEFEED documentation for details.
`)
	f.StringVar(&c.Strategy, "strategy", "sticky", "Kafka consumer group re-balance strategy")
	f.StringArrayVar(&c.Topics, "topic", nil, "the topic(s) that the consumer should use")

	// SASL
	f.StringVar(&c.saslClientID, "saslClientId", "", "client ID for OAuth authentication from a third-party provider")
	f.StringVar(&c.saslClientSecret, "saslClientSecret", "", "Client secret for OAuth authentication from a third-party provider")
	f.StringVar(&c.saslGrantType, "saslGrantType", "", "Override the default OAuth client credentials grant type for other implementations")
	f.StringVar(&c.saslMechanism, "saslMechanism", "", "Can be set to OAUTHBEARER, SCRAM-SHA-256, SCRAM-SHA-512, or PLAIN")
	f.StringArrayVar(&c.saslScopes, "saslScope", nil, "Scopes that the OAuth token should have access for.")
	f.StringVar(&c.saslTokenURL, "saslTokenURL", "", "Client token URL for OAuth authentication from a third-party provider")
	f.StringVar(&c.saslUser, "saslUser", "", "SASL username")
	f.StringVar(&c.saslPassword, "saslPassword", "", "SASL password")
}

// Preflight updates the configuration with sane defaults or returns an
// error if there are missing options for which a default cannot be
// provided.
func (c *Config) Preflight(ctx context.Context) error {
	if err := c.DLQ.Preflight(); err != nil {
		return err
	}
	if err := c.Script.Preflight(); err != nil {
		return err
	}
	if err := c.Sequencer.Preflight(); err != nil {
		return err
	}
	if err := c.Staging.Preflight(); err != nil {
		return err
	}
	if err := c.Target.Preflight(); err != nil {
		return err
	}
	if c.TargetSchema.Empty() {
		return errors.New("no target schema specified")
	}
	return c.preflight(ctx)
}

func (c *Config) preflight(ctx context.Context) error {
	if c.Group == "" {
		return errors.New("no group was configured")
	}
	if len(c.Brokers) == 0 {
		return errors.New("no brokers were configured")
	}
	if len(c.Topics) == 0 {
		return errors.New("no topics were configured")
	}
	var err error
	minTimestamp := hlc.New(0, 0)
	if len(c.MinTimestamp) != 0 {
		if minTimestamp, err = hlc.Parse(c.MinTimestamp); err != nil {
			return err
		}
	}
	maxTimestamp := hlc.New(math.MaxInt64, math.MaxInt)
	if len(c.MaxTimestamp) != 0 {
		if maxTimestamp, err = hlc.Parse(c.MaxTimestamp); err != nil {
			return err
		}
	}
	// We use ResolvedInterval when seeking the offset of resolved timestamps.
	// It should match the interval set when creating the changefeed, to minimize
	// the number of messages that we need to read in the Kafka feed.
	if c.ResolvedInterval < time.Millisecond {
		return errors.New("resolved interval must be at least 1 millisecond")
	}
	if hlc.Compare(minTimestamp, maxTimestamp) > 0 {
		return errors.New("minTimestamp must be before maxTimestamp")
	}
	c.timeRange = hlc.RangeExcluding(minTimestamp, maxTimestamp)
	log.Infof("Kafka time range %s", c.timeRange)
	sc := sarama.NewConfig()
	switch c.Strategy {
	case "sticky":
		sc.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}
	case "roundrobin":
		sc.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	case "range":
		sc.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
	default:
		return errors.Errorf("unrecognized consumer rebalance strategy: %s", c.Strategy)
	}

	if err := c.TLS.Preflight(); err != nil {
		return err
	}
	sc.Net.TLS.Config = c.TLS.AsTLSConfig()
	sc.Net.TLS.Enable = sc.Net.TLS.Config != nil
	// if saslMechanism is not null, then authentication is done via SASL.
	if c.saslMechanism != "" {
		sc.Net.SASL.Enable = true
		switch c.saslMechanism {
		case sarama.SASLTypeSCRAMSHA512:
			sc.Net.SASL.SCRAMClientGeneratorFunc = sha512ClientGenerator
		case sarama.SASLTypeSCRAMSHA256:
			sc.Net.SASL.SCRAMClientGeneratorFunc = sha256ClientGenerator
		case sarama.SASLTypeOAuth:
			var err error
			sc.Net.SASL.TokenProvider, err = c.newTokenProvider(ctx)
			if err != nil {
				return err
			}
		}
		sc.Net.SASL.Mechanism = sarama.SASLMechanism(c.saslMechanism)
		sc.Net.SASL.User = c.saslUser
		sc.Net.SASL.Password = c.saslPassword
		log.Infof("Using SASL %s", c.saslMechanism)
	}
	sc.Consumer.Offsets.Initial = sarama.OffsetOldest
	c.saramaConfig = sc
	return sc.Validate()
}

func (c *Config) newTokenProvider(ctx context.Context) (sarama.AccessTokenProvider, error) {
	// grant_type is by default going to be set to 'client_credentials' by the
	// clientcredentials library as defined by the spec, however non-compliant
	// auth server implementations may want a custom type
	var endpointParams url.Values
	if c.saslGrantType != `` {
		endpointParams = url.Values{"grant_type": {c.saslGrantType}}
	}
	if c.saslTokenURL == "" {
		return nil, errors.New("OAUTH2 requires a token URL")

	}
	tokenURL, err := url.Parse(c.saslTokenURL)
	if err != nil {
		return nil, errors.Wrap(err, "malformed token url")
	}
	if c.saslClientID == "" {
		return nil, errors.New("OAUTH2 requires a client id")

	}
	if c.saslClientSecret == "" {
		return nil, errors.New("OAUTH2 requires a client secret")
	}
	// the clientcredentials.Config's TokenSource method creates an
	// oauth2.TokenSource implementation which returns tokens for the given
	// endpoint, returning the same cached result until its expiration has been
	// reached, and then once expired re-requesting a new token from the endpoint.
	cfg := clientcredentials.Config{
		ClientID:       c.saslClientID,
		ClientSecret:   c.saslClientSecret,
		TokenURL:       tokenURL.String(),
		Scopes:         c.saslScopes,
		EndpointParams: endpointParams,
	}
	return &tokenProvider{
		tokenSource: cfg.TokenSource(ctx),
	}, nil
}
