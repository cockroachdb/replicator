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

package sinkprod

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/pflag"
)

// CommonConfig defines settings that are shared by [StagingConfig] and
// [TargetConfig].
type CommonConfig struct {
	// Connection string for the database.
	Conn string
	// The maximum lifetime of an idle connection.
	IdleTime time.Duration
	// The length of time to jitter disconnections over.
	JitterTime time.Duration
	// The maximum lifetime for a database connection; improves
	// loadbalancer compatibility.
	MaxLifetime time.Duration
	// The number of connections to the target database. If zero, a
	// default value will be used.
	MaxPoolSize int
}

func (c *CommonConfig) bind(f *pflag.FlagSet, prefix string) {
	f.StringVar(&c.Conn, prefix+"Conn", "",
		fmt.Sprintf("the %s database's connection string", prefix))
	f.DurationVar(&c.IdleTime, prefix+"IdleTime", defaultIdleTime,
		"maximum lifetime of an idle connection")
	f.DurationVar(&c.JitterTime, prefix+"JitterTime", defaultJitterTime,
		"the time over which to jitter database pool disconnections")
	f.DurationVar(&c.MaxLifetime, prefix+"MaxLifetime", defaultMaxLifetime,
		"the maximum lifetime of a database connection")
	f.IntVar(&c.MaxPoolSize, prefix+"MaxPoolSize", defaultMaxPoolSize,
		fmt.Sprintf("the maximum number of %s database connections", prefix))
}

func (c *CommonConfig) preflight(prefix string, requireConn bool) error {
	if requireConn {
		if c.Conn == "" {
			return errors.Errorf("%sConn must be set", prefix)
		}
	}
	if c.IdleTime == 0 {
		c.IdleTime = defaultIdleTime
	}
	if c.JitterTime == 0 {
		c.JitterTime = defaultJitterTime
	}
	if c.MaxPoolSize == 0 {
		c.MaxPoolSize = defaultMaxPoolSize
	}
	return nil
}
