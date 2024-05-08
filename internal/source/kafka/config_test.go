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
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/secure"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var maxRange = hlc.RangeExcluding(hlc.New(0, 0), hlc.New(math.MaxInt64, math.MaxInt))

// TestPreflight verifies that the kafka configuration can be validated
// before we start the service.
func TestPreflight(t *testing.T) {
	tlsConfig := secure.Config{
		ClientCert: "./testdata/test.crt",
		ClientKey:  "./testdata/test.key",
		CaCert:     "./testdata/ca.crt",
	}
	ctx := context.Background()
	a := assert.New(t)
	r := require.New(t)
	tests := []struct {
		name      string
		in        *Config
		strategy  []sarama.BalanceStrategy
		timeRange hlc.Range
		tls       bool
		sasl      bool
		saslMech  sarama.SASLMechanism
		wantErr   string
	}{
		{
			name: "no group",
			in: &Config{
				Brokers:          []string{"mybroker"},
				ResolvedInterval: time.Second,
				Topics:           []string{"mytopic"},
			},
			wantErr: "no group was configured",
		},
		{
			name: "no brokers",
			in: &Config{
				Group:            "mygroup",
				ResolvedInterval: time.Second,
				Topics:           []string{"mytopic"},
			},
			wantErr: "no brokers were configured",
		},
		{
			name: "no topics",
			in: &Config{
				Group:            "mygroup",
				Brokers:          []string{"mybroker"},
				ResolvedInterval: time.Second,
			},
			wantErr: "no topics were configured",
		},
		{
			name: "no strategy",
			in: &Config{
				Group:            "mygroup",
				Brokers:          []string{"mybroker"},
				ResolvedInterval: time.Second,
				Topics:           []string{"mytopic"},
			},
			wantErr: "unrecognized consumer rebalance strategy",
		},
		{
			name: "sticky",
			in: &Config{
				Group:            "mygroup",
				Brokers:          []string{"mybroker"},
				ResolvedInterval: time.Second,
				Topics:           []string{"mytopic"},
				Strategy:         "sticky",
			},
			strategy:  []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()},
			timeRange: maxRange,
		},
		{
			name: "roundrobin",
			in: &Config{
				Group:            "mygroup",
				Brokers:          []string{"mybroker"},
				ResolvedInterval: time.Second,
				Topics:           []string{"mytopic"},
				Strategy:         "roundrobin",
			},
			strategy:  []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()},
			timeRange: maxRange,
		},
		{
			name: "range",
			in: &Config{
				Group:            "mygroup",
				Brokers:          []string{"mybroker"},
				ResolvedInterval: time.Second,
				Topics:           []string{"mytopic"},
				Strategy:         "range",
			},
			strategy:  []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()},
			timeRange: maxRange,
		},
		{
			name: "time range",
			in: &Config{
				Group:            "mygroup",
				Brokers:          []string{"mybroker"},
				Topics:           []string{"mytopic"},
				Strategy:         "sticky",
				ResolvedInterval: time.Second,
				MinTimestamp:     "1.0",
				MaxTimestamp:     "2.0",
			},
			strategy:  []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()},
			timeRange: hlc.RangeExcluding(hlc.New(1, 0), hlc.New(2, 0)),
		},
		{
			name: "interval too small",
			in: &Config{
				Group:            "mygroup",
				Brokers:          []string{"mybroker"},
				Topics:           []string{"mytopic"},
				Strategy:         "sticky",
				ResolvedInterval: time.Nanosecond,
				MinTimestamp:     "1.0",
				MaxTimestamp:     "2.0",
			},
			wantErr: "resolved interval must be at least 1 millisecond",
		},
		{
			name: "tls",
			in: &Config{
				Group:            "mygroup",
				Brokers:          []string{"mybroker"},
				ResolvedInterval: time.Second,
				Topics:           []string{"mytopic"},
				Strategy:         "sticky",
				TLS:              tlsConfig,
			},
			strategy:  []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()},
			timeRange: maxRange,
			tls:       true,
		},
		{
			name: "missing user",
			in: &Config{
				Group:            "mygroup",
				Brokers:          []string{"mybroker"},
				ResolvedInterval: time.Second,
				Topics:           []string{"mytopic"},
				Strategy:         "sticky",
				TLS:              tlsConfig,
				saslMechanism:    sarama.SASLTypePlaintext,
			},
			wantErr: "invalid configuration (Net.SASL.User must not be empty when SASL is enabled)",
		},
		{
			name: "plain",
			in: &Config{
				Group:            "mygroup",
				Brokers:          []string{"mybroker"},
				ResolvedInterval: time.Second,
				Topics:           []string{"mytopic"},
				Strategy:         "sticky",
				TLS:              tlsConfig,
				saslMechanism:    sarama.SASLTypePlaintext,
				saslUser:         "user",
				saslPassword:     "pass",
			},
			strategy:  []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()},
			timeRange: maxRange,
			tls:       true,
			sasl:      true,
			saslMech:  sarama.SASLTypePlaintext,
		},
		{
			name: "scram256",
			in: &Config{
				Group:            "mygroup",
				Brokers:          []string{"mybroker"},
				ResolvedInterval: time.Second,
				Topics:           []string{"mytopic"},
				Strategy:         "sticky",
				TLS:              tlsConfig,
				saslMechanism:    sarama.SASLTypeSCRAMSHA256,
				saslUser:         "user",
				saslPassword:     "pass",
			},
			strategy:  []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()},
			timeRange: maxRange,
			tls:       true,
			sasl:      true,
			saslMech:  sarama.SASLTypeSCRAMSHA256,
		},
		{
			name: "scram512",
			in: &Config{
				Group:            "mygroup",
				Brokers:          []string{"mybroker"},
				ResolvedInterval: time.Second,
				Topics:           []string{"mytopic"},
				Strategy:         "sticky",
				TLS:              tlsConfig,
				saslMechanism:    sarama.SASLTypeSCRAMSHA512,
				saslUser:         "user",
				saslPassword:     "pass",
			},
			strategy:  []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()},
			timeRange: maxRange,
			tls:       true,
			sasl:      true,
			saslMech:  sarama.SASLTypeSCRAMSHA512,
		},
		{
			name: "oath2",
			in: &Config{
				Group:            "mygroup",
				Brokers:          []string{"mybroker"},
				ResolvedInterval: time.Second,
				Topics:           []string{"mytopic"},
				Strategy:         "sticky",
				TLS:              tlsConfig,
				saslMechanism:    sarama.SASLTypeOAuth,
				saslTokenURL:     "http://example.com",
				saslClientID:     "myclient",
				saslClientSecret: "mysecret",
			},
			strategy:  []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()},
			timeRange: maxRange,
			tls:       true,
			sasl:      true,
			saslMech:  sarama.SASLTypeOAuth,
		},
		{
			name: "oath2 no token",
			in: &Config{
				Group:            "mygroup",
				Brokers:          []string{"mybroker"},
				ResolvedInterval: time.Second,
				Topics:           []string{"mytopic"},
				Strategy:         "sticky",
				TLS:              tlsConfig,
				saslMechanism:    sarama.SASLTypeOAuth,
				saslClientID:     "myclient",
				saslClientSecret: "mysecret",
			},
			wantErr: "OAUTH2 requires a token URL",
		},
		{
			name: "oath2 no client",
			in: &Config{
				Group:            "mygroup",
				Brokers:          []string{"mybroker"},
				ResolvedInterval: time.Second,
				Topics:           []string{"mytopic"},
				Strategy:         "sticky",
				TLS:              tlsConfig,
				saslMechanism:    sarama.SASLTypeOAuth,
				saslTokenURL:     "http://example.com",
				saslClientID:     "myclient",
			},
			wantErr: "OAUTH2 requires a client secret",
		},
		{
			name: "oath2",
			in: &Config{
				Group:            "mygroup",
				Brokers:          []string{"mybroker"},
				ResolvedInterval: time.Second,
				Topics:           []string{"mytopic"},
				Strategy:         "sticky",
				TLS:              tlsConfig,
				saslMechanism:    sarama.SASLTypeOAuth,
				saslTokenURL:     "http://example.com",
				saslClientSecret: "mysecret",
			},
			wantErr: "OAUTH2 requires a client id",
		},
	}
	t.Parallel()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := tt.in
			err := config.preflight(ctx)
			if tt.wantErr != "" {
				a.ErrorContains(err, tt.wantErr)
				return
			}
			a.NoError(err)
			r.Equal(len(tt.strategy), len(config.saramaConfig.Consumer.Group.Rebalance.GroupStrategies))
			for i, s := range tt.strategy {
				a.IsType(s, config.saramaConfig.Consumer.Group.Rebalance.GroupStrategies[i])
			}
			a.Equal(tt.timeRange, config.timeRange)
			a.Equal(tt.tls, config.saramaConfig.Net.TLS.Enable)
			if tt.tls {
				a.NotEmpty(config.saramaConfig.Net.TLS.Config)
			}
			a.Equal(tt.sasl, config.saramaConfig.Net.SASL.Enable)
			if tt.sasl {
				a.Equal(tt.saslMech, config.saramaConfig.Net.SASL.Mechanism)
				switch tt.saslMech {
				case sarama.SASLTypeSCRAMSHA256:
					a.IsType(sha256ClientGenerator,
						config.saramaConfig.Net.SASL.SCRAMClientGeneratorFunc)
					a.Empty(config.saramaConfig.Net.SASL.TokenProvider)
				case sarama.SASLTypeSCRAMSHA512:
					a.IsType(sha512ClientGenerator,
						config.saramaConfig.Net.SASL.SCRAMClientGeneratorFunc)
					a.Empty(config.saramaConfig.Net.SASL.TokenProvider)
				case sarama.SASLTypeOAuth:
					a.IsType(&tokenProvider{},
						config.saramaConfig.Net.SASL.TokenProvider)
					a.NotEmpty(config.saslTokenURL)
					_, err := url.Parse(config.saslTokenURL)
					a.NoError(err)
					a.Empty(config.saramaConfig.Net.SASL.SCRAMClientGeneratorFunc)
				default:
					a.Empty(config.saramaConfig.Net.SASL.SCRAMClientGeneratorFunc)
					a.Empty(config.saramaConfig.Net.SASL.TokenProvider)
				}
			}
		})
	}
}
