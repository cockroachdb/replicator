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
import java.io.StringWriter;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine.RecordCommitter;

public class Batcher extends BaseChangeConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(Batcher.class);
   
    private StringWriter buffer = new StringWriter();

    public Batcher() {
        super();
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
    public synchronized void handleBatch(List<ChangeEvent<String, String>> records,
            RecordCommitter<ChangeEvent<String, String>> committer) throws InterruptedException {
        LOGGER.debug("Got {} events\n", records.size());
        try {
            buffer.getBuffer().setLength(0);
            Batch batch = new Batch(buffer);
            for (ChangeEvent<String, String> r : records) {
                batch.next();
                batch.key(r.key());
                batch.value(r.value());
            }
            batch.end();
            write(buffer.toString());
            post(buffer.toString());
            markProcessed(records, committer);
        } catch (IOException e) {
            throw new DebeziumException(e);
        }
    }
   
}
