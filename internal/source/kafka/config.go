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
	"math"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/sequencer"
	"github.com/cockroachdb/cdc-sink/internal/sinkprod"
	"github.com/cockroachdb/cdc-sink/internal/target/dlq"
	"github.com/cockroachdb/cdc-sink/internal/util/hlc"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
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

	BatchSize    int      // How many messages to accumulate before committing to the target
	Brokers      []string // The address of the Kafka brokers
	Group        string   // the Kafka consumer group id.
	MaxTimestamp string   // Only accept messages at or older than this timestamp
	MinTimestamp string   // Only accept messages at or newer than this timestamp
	Strategy     string   // Kafka consumer group re-balance strategy
	Topics       []string // The list of topics that the consumer should use.

	// The following are computed.

	// Timestamp range, computed based on minTimestamp and maxTimestamp.
	timeRange hlc.Range

	// Kafka consumer group re-balance strategy
	rebalanceStrategy []sarama.BalanceStrategy
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
	f.StringVar(&c.Strategy, "strategy", "sticky", "Kafka consumer group re-balance strategy")
	f.StringArrayVar(&c.Topics, "topic", nil, "the topic(s) that the consumer should use")
}

// Preflight updates the configuration with sane defaults or returns an
// error if there are missing options for which a default cannot be
// provided.
func (c *Config) Preflight() error {
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
	maxTimestamp := hlc.New(math.MaxInt64, math.MaxInt-1)
	if len(c.MaxTimestamp) != 0 {
		if maxTimestamp, err = hlc.Parse(c.MaxTimestamp); err != nil {
			return err
		}
	}
	if hlc.Compare(minTimestamp, maxTimestamp) > 0 {
		return errors.New("minTimestamp must be before maxTimestamp")
	}
	c.timeRange = hlc.RangeIncluding(minTimestamp, maxTimestamp)
	log.Infof("Kafka time range %s", c.timeRange)
	switch c.Strategy {
	case "sticky":
		c.rebalanceStrategy = []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}
	case "roundrobin":
		c.rebalanceStrategy = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	case "range":
		c.rebalanceStrategy = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
	default:
		return errors.Errorf("Unrecognized consumer rebalance strategy: %s", c.Strategy)
	}
	return nil
}
