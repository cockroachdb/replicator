// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jwt

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	jwtRefreshedAt = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "jwt_last_refresh_time",
		Help: "the unix timestamp at which the JWT caches were refreshed",
	})
)
