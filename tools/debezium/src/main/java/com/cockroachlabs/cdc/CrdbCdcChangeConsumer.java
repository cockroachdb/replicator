// Copyright 2023 The Cockroach Authors
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
package com.cockroachlabs.cdc;

import java.io.IOException;
import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine.RecordCommitter;

public class CrdbCdcChangeConsumer extends BaseChangeConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(Batcher.class);

    HttpClient client = null;
    private ObjectMapper objectMapper;

    public CrdbCdcChangeConsumer() {
        super();
        objectMapper = new ObjectMapper();
    }

    /**
     * Processes the change events.
     *
     * @param records   the change events, as serialized key-value pair of JSON
     *                  objects.
     * @param committer keeps track of the events that have been successfully
     *                  processed.
     * @throws InterruptedException
     */
    @Override
    public void handleBatch(List<ChangeEvent<String, String>> records,
            RecordCommitter<ChangeEvent<String, String>> committer) throws InterruptedException {
        LOGGER.debug("Got {} events\n", records.size());
        // CREATE CHANGEFEED INTO
        // 'webhook-https://127.0.0.1:58856/tgt-80741-90/public/tbl-10
        // {"payload": [
        // {"after":{"pk":4,"v":5},
        // "key":[4],
        // "topic":"tt",
        // "updated":"1701985873781308000.0000000000"}
        // ],"length":1}
        try {
            ArrayNode objects = objectMapper.createArrayNode();
            String resolved = null;
            for (ChangeEvent<String, String> r : records) {
                LOGGER.debug("{\"key\": {}\n\"value\":{}", r.key(), r.value());
                ObjectNode obj = objectMapper.createObjectNode();
                if (r.value() == null || r.value().isEmpty()) {
                    continue;
                }
                JsonNode value = objectMapper.readTree(r.value());
                JsonNode status = value.get("status");
                if (status != null && status.asText().equals("END")) {
                    resolved = timestamp(value);
                }
                JsonNode source = value.get("source");
                if (source == null) {
                    LOGGER.debug("skipping {}. No source", r.value());
                    continue;
                }
                JsonNode after = value.get("after");
                JsonNode before = value.get("before");
                String op = value.get("op").asText();
                switch (op) {
                    case "d":
                    case "u":
                    case "r":
                    case "c":
                        obj.set("after", after);
                        obj.set("before", before);
                        break;
                    default:
                        LOGGER.debug("skipping {}. No op", r.value());
                        // CDC endpoint doesn't end other stuff.
                        continue;
                }
                JsonNode key = objectMapper.readTree(r.key());
                obj.set("key", getKey(key));
                obj.put("updated", timestamp(source));
                obj.put("topic", source.get("table").toString());
                LOGGER.info("adding {}", r.value());
                objects.add(obj);
            }
            ObjectNode result = objectMapper.createObjectNode();
            result.set("payload", objects);
            result.put("length", objects.size());
            String buffer = result.toString();
            write(buffer);
            post(buffer);
            if (resolved != null) {
                write(resolved);
                postResolved(resolved);
            }
            markProcessed(records, committer);
            committer.markBatchFinished();
        } catch (IOException e) {
            throw new DebeziumException(e);
        }
    }

    /**
     * @param props
     */
    @Override
    public void setRequiredProperties(Properties props) {
        super.setRequiredProperties(props);
        // We want transaction boundaries.
        props.setProperty("provide.transaction.metadata", "true");
    }

    private JsonNode getKey(JsonNode key) throws JsonProcessingException {
        ArrayNode result = objectMapper.createArrayNode();
        List<String> keys = new ArrayList<String>();
        // We determine the order of the keys based on the schema.
        JsonNode schema = key.get("schema");
        JsonNode fields = schema.get("fields");
        Iterator<JsonNode> itr = fields.elements();
        while (itr.hasNext()) {
            JsonNode a = itr.next();
            keys.add(a.get("field").asText());
        }
        JsonNode payload = key.get("payload");
        Iterator<String> k = keys.iterator();
        while (k.hasNext()) {
            JsonNode v = payload.get(k.next());
            result.add(v);
        }
        return result;
    }

    private void postResolved(String timestamp) throws IOException, InterruptedException {
        ObjectNode result = objectMapper.createObjectNode();
        result.put("resolved", timestamp);
        post(result.toString());
    }

    private String timestamp(JsonNode node) {
        return node.get("ts_ms").asText() + "000000.000";
    }
}
