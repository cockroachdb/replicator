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

package workload

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/cockroachdb/replicator/internal/source/cdc"
	"github.com/cockroachdb/replicator/internal/util/lockset"
	"github.com/cockroachdb/replicator/internal/util/stopper"
	"github.com/cockroachdb/replicator/internal/util/workgroup"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// The sender handles communication between the workload generator and
// the target (load-balanced) Replicator instance.
//
// The sender manages an internal, key-ordered queue to handle network
// or backend errors.
type sender struct {
	cfg    *clientConfig
	client *http.Client
	queue  *lockset.Set[string] // Ensure we maintain per-key ordering.
}

func newSender(ctx context.Context, cfg *clientConfig) (*sender, error) {
	queue, err := lockset.New[string](
		workgroup.WithSize(ctx, cfg.concurrentRequests, cfg.concurrentRequests),
		"sender",
	)
	if err != nil {
		return nil, err
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	if skip, _ := strconv.ParseBool(cfg.urlParsed.Query().Get("insecure_tls_skip_verify")); skip {
		transport.TLSClientConfig = &tls.Config{
			InsecureSkipVerify: skip,
		}
	}

	return &sender{
		cfg: cfg,
		client: &http.Client{
			Timeout:   cfg.requestTimeout,
			Transport: transport,
		},
		queue: queue,
	}, nil
}

// Send returns a task handle.
func (s *sender) Send(ctx *stopper.Context, payload *cdc.WebhookPayload) lockset.Outcome {
	var keys []string

	if payload.Resolved != "" {
		// Resolved timestamp payloads are sequenced by the runner, so
		// they need only to maintain order with respect to one another.
		keys = []string{"__RESOLVED__"}
	} else {
		// We need to maintain per-key order for updates.
		keys = make([]string, len(payload.Payload))
		for i := range keys {
			keys[i] = string(payload.Payload[i].Key)
		}
	}
	data, err := json.Marshal(payload)
	if err != nil {
		ret := lockset.NewOutcome()
		ret.Set(lockset.StatusFor(err))
		return ret
	}
	outcome, _ := s.queue.Schedule(keys, func(keys []string) error {
		retry := s.cfg.retryMin
		for {
			req, err := http.NewRequestWithContext(ctx,
				http.MethodPost, s.cfg.url, bytes.NewReader(data))
			if err != nil {
				return errors.WithStack(err)
			}
			if s.cfg.token != "" {
				req.Header.Add("Authorization", "Bearer "+s.cfg.token)
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := s.client.Do(req)
			if err == nil && resp.StatusCode == http.StatusOK {
				return nil
			}
			// Don't bother with error reporting if we're shutting down.
			// The HTTP listener is closed asynchronously, so we'll see
			// connection-refused errors after a ^C.
			if ctx.IsStopping() {
				return nil
			}
			// For testing, we want to immediately fail.
			if s.cfg.failFast {
				if err == nil {
					err = errors.Errorf("HTTP request failed: %d %s",
						resp.StatusCode, resp.Status)
				}
				return err
			}
			if err == nil {
				log.Warnf("HTTP request failed: %d %s; retrying in %s",
					resp.StatusCode, resp.Status, retry)
			} else {
				log.WithError(err).Warnf("HTTP request failed; retrying in %s", retry)
			}
			select {
			case <-time.After(retry):
				retry = min(retry*2, s.cfg.retryMax)
			case <-ctx.Stopping():
				return nil
			}
		}
	})
	return outcome
}
