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
	"github.com/cockroachdb/replicator/internal/conveyor"
	"github.com/cockroachdb/replicator/internal/util/hlc"
	"github.com/cockroachdb/replicator/internal/util/secure"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	maxRange              = hlc.RangeExcluding(hlc.New(0, 0), hlc.New(math.MaxInt64, math.MaxInt))
	defaultConvoyerConfig = conveyor.Config{
		BestEffortWindow: conveyor.DefaultBestEffortWindow,
		BestEffortOnly:   false,
		Immediate:        false,
	}
)

func TestBind(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want *Config
	}{
		{
			name: "tls ca",
			args: []string{
				"--group", "test",
				"--broker", "localhost:1000",
				"--topic", "topic",
				"--tlsCACertificate", "./testdata/ca.crt"},
			want: &Config{
				Conveyor: defaultConvoyerConfig,
				TLS: secure.Config{
					CaCert: "./testdata/ca.crt",
				},
			},
		},
		{
			name: "tls ca + keypair",
			args: []string{
				"--group", "test",
				"--broker", "localhost:1000",
				"--topic", "topic",
				"--tlsCACertificate", "./testdata/ca.crt",
				"--tlsCertificate", "./testdata/test.crt",
				"--tlsPrivateKey", "./testdata/test.key"},
			want: &Config{
				Conveyor: defaultConvoyerConfig,
				TLS: secure.Config{
					CaCert:     "./testdata/ca.crt",
					ClientCert: "./testdata/test.crt",
					ClientKey:  "./testdata/test.key",
				},
			},
		},
		{
			name: "immediate",
			args: []string{
				"--group", "test",
				"--broker", "localhost:1000",
				"--topic", "topic",
				"--immediate",
			},
			want: &Config{
				Conveyor: conveyor.Config{
					BestEffortWindow: conveyor.DefaultBestEffortWindow,
					BestEffortOnly:   false,
					Immediate:        true},
			},
		},
		{
			name: "best effort",
			args: []string{
				"--group", "test",
				"--broker", "localhost:1000",
				"--topic", "topic",
				"--bestEffortOnly",
			},
			want: &Config{
				Conveyor: conveyor.Config{
					BestEffortWindow: conveyor.DefaultBestEffortWindow,
					BestEffortOnly:   true,
					Immediate:        false},
			},
		},
		{
			name: "scram256",
			args: []string{
				"--group", "test",
				"--broker", "localhost:1000",
				"--topic", "topic",
				"--saslMechanism", "SCRAM-SHA-256",
				"--saslUser", "test",
				"--saslPassword", "test",
			},
			want: &Config{
				Conveyor: defaultConvoyerConfig,
				SASL: SASLConfig{
					Mechanism: "SCRAM-SHA-256",
					Password:  "test",
					User:      "test",
				},
			},
		},
		{
			name: "scram512",
			args: []string{
				"--group", "test",
				"--broker", "localhost:1000",
				"--topic", "topic",
				"--saslMechanism", "SCRAM-SHA-512",
				"--saslUser", "test",
				"--saslPassword", "test",
			},
			want: &Config{
				Conveyor: defaultConvoyerConfig,
				SASL: SASLConfig{
					Mechanism: "SCRAM-SHA-512",
					Password:  "test",
					User:      "test",
				},
			},
		},
		{
			name: "oauth",
			args: []string{
				"--group", "test",
				"--broker", "localhost:1000",
				"--topic", "topic",
				"--saslClientID", "myclient",
				"--saslClientSecret", "mysecret",
				"--saslMechanism", "OAUTHBEARER",
				"--saslTokenURL", "http://example.com",
			},
			want: &Config{
				Conveyor: defaultConvoyerConfig,
				SASL: SASLConfig{
					ClientID:     "myclient",
					ClientSecret: "mysecret",
					Mechanism:    "OAUTHBEARER",
					TokenURL:     "http://example.com",
				},
			},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	t.Parallel()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			a := assert.New(t)
			flags := &pflag.FlagSet{}
			config := &Config{}
			config.Bind(flags)
			err := flags.Parse(test.args)
			a.NoError(err)

			a.Equal(test.want.TLS, config.TLS)
			a.Equal(test.want.SASL, config.SASL)
			a.Equal(test.want.Conveyor, config.Conveyor)
			a.NoError(config.preflight(ctx))
		})
	}
}

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
				SASL: SASLConfig{
					Mechanism: sarama.SASLTypePlaintext,
				},
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
				SASL: SASLConfig{
					Mechanism: sarama.SASLTypePlaintext,
					User:      "user",
					Password:  "pass",
				},
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
				SASL: SASLConfig{
					Mechanism: sarama.SASLTypeSCRAMSHA256,
					User:      "user",
					Password:  "pass",
				},
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
				SASL: SASLConfig{
					Mechanism: sarama.SASLTypeSCRAMSHA512,
					User:      "user",
					Password:  "pass",
				},
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
				SASL: SASLConfig{
					Mechanism:    sarama.SASLTypeOAuth,
					TokenURL:     "http://example.com",
					ClientID:     "myclient",
					ClientSecret: "mysecret",
				},
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
				SASL: SASLConfig{
					Mechanism:    sarama.SASLTypeOAuth,
					ClientID:     "myclient",
					ClientSecret: "mysecret",
				},
			},
			wantErr: "OAUTH2 requires a token URL",
		},
		{
			name: "oath2 no client secret",
			in: &Config{
				Group:            "mygroup",
				Brokers:          []string{"mybroker"},
				ResolvedInterval: time.Second,
				Topics:           []string{"mytopic"},
				Strategy:         "sticky",
				TLS:              tlsConfig,
				SASL: SASLConfig{
					Mechanism: sarama.SASLTypeOAuth,
					TokenURL:  "http://example.com",
					ClientID:  "myclient",
				},
			},
			wantErr: "OAUTH2 requires a client secret",
		},
		{
			name: "oath2 no client id",
			in: &Config{
				Group:            "mygroup",
				Brokers:          []string{"mybroker"},
				ResolvedInterval: time.Second,
				Topics:           []string{"mytopic"},
				Strategy:         "sticky",
				TLS:              tlsConfig,
				SASL: SASLConfig{
					Mechanism:    sarama.SASLTypeOAuth,
					TokenURL:     "http://example.com",
					ClientSecret: "mysecret",
				},
			},
			wantErr: "OAUTH2 requires a client id",
		},
	}
	t.Parallel()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := test.in
			err := config.preflight(ctx)
			if test.wantErr != "" {
				a.ErrorContains(err, test.wantErr)
				return
			}
			a.NoError(err)
			r.Equal(len(test.strategy), len(config.saramaConfig.Consumer.Group.Rebalance.GroupStrategies))
			for i, s := range test.strategy {
				a.IsType(s, config.saramaConfig.Consumer.Group.Rebalance.GroupStrategies[i])
			}
			a.Equal(test.timeRange, config.timeRange)
			a.Equal(test.tls, config.saramaConfig.Net.TLS.Enable)
			if test.tls {
				a.NotEmpty(config.saramaConfig.Net.TLS.Config)
			}
			a.Equal(test.sasl, config.saramaConfig.Net.SASL.Enable)
			if test.sasl {
				a.Equal(test.saslMech, config.saramaConfig.Net.SASL.Mechanism)
				switch test.saslMech {
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
					a.NotEmpty(config.SASL.TokenURL)
					_, err := url.Parse(config.SASL.TokenURL)
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
