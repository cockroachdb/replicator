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
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"

	"github.com/IBM/sarama"
	"github.com/golang/groupcache/lru"
	"github.com/linkedin/goavro/v2"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	contentType = "application/vnd.schemaregistry.v1+json"
	schemaByID  = "/schemas/ids/%d"
)

type registry struct {
	servers []string
	client  *http.Client
	retries int
	mu      struct {
		sync.Mutex
		cache *lru.Cache
	}
}

var _ Decoder = &registry{}

type schemaResponse struct {
	Schema string `json:"schema"`
}

func newRegistry(servers []string) *registry {
	cache := &lru.Cache{}
	registry := &registry{
		servers: servers,
		client:  &http.Client{},
		retries: 10,
	}
	registry.mu.cache = cache
	return registry
}

func (r *registry) Decode(msg *sarama.ConsumerMessage) (*Payload, error) {
	var res avroPayload
	key, err := r.decode(msg.Key)
	if err != nil {
		return nil, err
	}
	payload := &Payload{Key: key}
	value, err := r.decode(msg.Value)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(value, &res); err != nil {
		// Empty input is a no-op.
		if errors.Is(err, io.EOF) {
			return payload, nil
		}
		return nil, errors.Wrap(err, "could not decode payload")
	}
	if res.Resolved != nil {
		var resolved map[string]string
		if err := json.Unmarshal(res.Resolved, &resolved); err != nil {
			return nil, errors.Wrap(err, "could not decode payload")
		}
		payload.Resolved = resolved["string"]
		return payload, nil
	}
	var updated map[string]string
	if err := json.Unmarshal(res.Updated, &updated); err != nil {
		return nil, errors.Wrap(err, "could not decode payload")
	}
	payload.Updated = updated["string"]
	if payload.After, err = extractMutation(res.After); err != nil {
		return nil, errors.Wrap(err, "could not decode payload")
	}
	if payload.Before, err = extractMutation(res.Before); err != nil {
		return nil, errors.Wrap(err, "could not decode payload")
	}
	return payload, nil
}

func (r *registry) getSchema(id int) (*goavro.Codec, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if v, ok := r.mu.cache.Get(id); ok {
		return v.(*goavro.Codec), nil
	}
	resp, err := r.httpCall("GET", fmt.Sprintf(schemaByID, id), nil)
	if err != nil {
		return nil, err
	}
	log.Infof("getSchema %s", string(resp))
	schema, err := parseSchema(resp)
	if err != nil {
		return nil, err
	}
	r.mu.cache.Add(id, schema)
	return schema, nil
}

func (r *registry) httpCall(method, uri string, payload io.Reader) ([]byte, error) {
	for i := 0; i < r.retries; i++ {
		url := fmt.Sprintf("%s%s", r.servers[rand.Intn(len(r.servers))], uri)
		req, err := http.NewRequest(method, url, payload)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", contentType)
		resp, err := r.client.Do(req)
		if err != nil {
			return nil, err
		}
		if retriable(resp) {
			resp.Body.Close()
			continue
		}
		defer resp.Body.Close()
		if !okStatus(resp) {
			return nil, errors.Errorf("schema registry failure %s", resp.Status)
		}
		return io.ReadAll(resp.Body)
	}
	return nil, errors.New("failed to connect to schema registry")
}

func (r *registry) decode(in []byte) ([]byte, error) {
	if in == nil {
		return nil, nil
	}
	log.Infof("decode %q", string(in))
	schemaId := binary.BigEndian.Uint32(in[1:5])
	codec, err := r.getSchema(int(schemaId))
	if err != nil {
		return nil, err
	}
	native, _, err := codec.NativeFromBinary(in[5:])
	if err != nil {
		return nil, err
	}
	// Convert native Go form to textual Avro data
	return codec.TextualFromNative(nil, native)
}

type avroPayload struct {
	After    json.RawMessage `json:"after"`
	Before   json.RawMessage `json:"before"`
	Resolved json.RawMessage `json:"resolved"`
	Updated  json.RawMessage `json:"updated"`
}

func extractMutation(msg []byte) (json.RawMessage, error) {
	res := make(map[string]json.RawMessage)
	var payload map[string]map[string]map[string]json.RawMessage
	dec := json.NewDecoder(bytes.NewReader(msg))
	if err := dec.Decode(&payload); err != nil {
		return nil, errors.Wrap(err, "could not decode payload")
	}
	// TODO (silvano): error handling
	for _, tables := range payload {
		// there should be only one table
		for col, values := range tables {
			for t, v := range values {
				log.Infof("extract mutation %s  %s %s", col, t, v)
				// there should be only one type?
				res[col] = v
			}
		}
	}
	return json.Marshal(res)
}
func parseSchema(str []byte) (*goavro.Codec, error) {
	var schema = new(schemaResponse)
	if err := json.Unmarshal(str, &schema); err != nil {
		return nil, err
	}
	return goavro.NewCodec(schema.Schema)
}

func retriable(resp *http.Response) bool {
	return resp.StatusCode >= 500 && resp.StatusCode < 600
}

func okStatus(resp *http.Response) bool {
	return resp.StatusCode >= 200 && resp.StatusCode < 400
}
