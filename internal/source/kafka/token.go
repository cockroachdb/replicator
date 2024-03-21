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
	"github.com/IBM/sarama"
	"golang.org/x/oauth2"
)

type tokenProvider struct {
	tokenSource oauth2.TokenSource
}

var _ sarama.AccessTokenProvider = (*tokenProvider)(nil)

// Token implements the sarama.AccessTokenProvider interface.  This is called by
// Sarama when connecting to the broker.
func (t *tokenProvider) Token() (*sarama.AccessToken, error) {
	token, err := t.tokenSource.Token()
	if err != nil {
		// Errors will result in Sarama retrying the broker connection and logging
		// the transient error, with a Broker connection error surfacing after retry
		// attempts have been exhausted.
		return nil, err
	}
	return &sarama.AccessToken{Token: token.AccessToken}, nil
}
