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
	"crypto/sha256"
	"crypto/sha512"

	"github.com/IBM/sarama"
	"github.com/xdg-go/scram"
)

var (
	// sha256ClientGenerator returns a SCRAMClient for the
	// SCRAM-SHA-256 SASL mechanism. This can used as a
	// SCRAMCLientGeneratorFunc when constructing a sarama SASL
	// configuration.
	sha256ClientGenerator = func() sarama.SCRAMClient {
		return &scramClient{HashGeneratorFcn: sha256.New}
	}

	// sha512ClientGenerator returns a SCRAMClient for the
	// SCRAM-SHA-512 SASL mechanism. This can used as a
	// SCRAMCLientGeneratorFunc when constructing a sarama SASL
	// configuration.
	sha512ClientGenerator = func() sarama.SCRAMClient {
		return &scramClient{HashGeneratorFcn: sha512.New}
	}
)

type scramClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

var _ sarama.SCRAMClient = &scramClient{}

// Begin prepares the client for the SCRAM exchange
// with the server with a user name and a password
func (c *scramClient) Begin(userName, password, authzID string) error {
	var err error
	c.Client, err = c.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	c.ClientConversation = c.Client.NewConversation()
	return nil
}

// Step steps client through the SCRAM exchange. It is
// called repeatedly until it errors or `Done` returns true.
func (c *scramClient) Step(challenge string) (string, error) {
	return c.ClientConversation.Step(challenge)
}

// Done should return true when the SCRAM conversation
// is over.
func (c *scramClient) Done() bool {
	return c.ClientConversation.Done()
}
