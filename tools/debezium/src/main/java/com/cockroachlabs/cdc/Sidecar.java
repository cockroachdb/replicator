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

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.Writer;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;

/**
 * Sidecar provides integration to replicator via a debezium connector.
 * The Sidecar connects to a source database based on the properties provided
 * and listens to change events. Once change events are received they are
 * forwarded to replicator for processing.
 * The properties define the connector to use and its configuration.
 * Refer to each connector
 * https://debezium.io/documentation/reference/stable/connectors/index.html
 * for specific configuration information.
 **/
public class Sidecar {
    private static final Logger LOGGER = LoggerFactory.getLogger(Sidecar.class);
    public static final String PROP_PREFIX = "cdcsink.";
    public static final String PROP_WEBHOOK_URL = "url";
    public static final String PROP_FILE = "file";
    public static final String PROP_SKIP_VERIFY = "skip.verify";
    public static final String PROP_CLIENT_TIMEOUT = "timeout.ms";
    public static final String PROP_RETRIES = "retries";
    public static final String PROP_RETRY_INTERVAL = "retry.interval.ms";

    private Properties props;
    private BaseChangeConsumer handler;

    public Sidecar(Properties props, BaseChangeConsumer handler) throws IOException, GeneralSecurityException {
        this.handler = handler;
        this.props = props;
        initWithConfig(props);
    }

    
    /** 
     * @param props
     * @throws IOException
     * @throws GeneralSecurityException
     */
    void initWithConfig(Properties props) throws IOException, GeneralSecurityException {
        String sinkUrl = props.getProperty(PROP_PREFIX + PROP_WEBHOOK_URL);
        if (sinkUrl != null) {
            LOGGER.info("cdc sink url:" + sinkUrl);
            Boolean skipVerify = false;
            String skipVerifyFlag = props.getProperty(PROP_PREFIX + PROP_SKIP_VERIFY);
            if (skipVerifyFlag != null) {
                skipVerify = Boolean.parseBoolean(skipVerifyFlag);
                LOGGER.info("cdc sink skipVerify:" + skipVerify);
            }
            handler.withService(sinkUrl, skipVerify);
        }
        String file = props.getProperty(PROP_PREFIX + PROP_FILE);
        if (file != null) {
            Writer w = new BufferedWriter(new FileWriter(file));
            handler.withWriter(w);
        }

        String timeout = props.getProperty(PROP_PREFIX + PROP_CLIENT_TIMEOUT);
        if (timeout != null) {
            handler.setTimeoutDuration(Duration.ofMillis(Long.parseLong(timeout)));
        }

        String rt = props.getProperty(PROP_PREFIX + PROP_RETRIES);
        if (timeout != null) {
            handler.setMaxRetries(Integer.parseInt(rt));
        }

        String rtInterval = props.getProperty(PROP_PREFIX + PROP_RETRY_INTERVAL);
        if (timeout != null) {
            handler.setRetryInterval(Duration.ofMillis(Long.parseLong(rtInterval)));
        }
    }

    
    /** 
     * @throws InterruptedException
     */
    public void start() throws InterruptedException {
        DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
                .using(props)
                .notifying((records, committer) -> handler.handleBatch(records, committer))
                .build();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);
        while (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
        }
    }

    public static void main(String[] args) {
        String propertiesFile = null;
        Options options = new Options();
        options.addRequiredOption("p", "properties", true, "Properties file");
        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse(options, args);
            propertiesFile = cmd.getOptionValue("properties");
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        }
        Configuration config = Configuration.empty();
        final Properties props = config.asProperties();
        try {
            props.load(new FileInputStream(propertiesFile));
        } catch (Exception e) {
            System.out.println(e);
            System.exit(1);
        }
        BaseChangeConsumer consumer = new CrdbCdcChangeConsumer();
        consumer.setRequiredProperties(props);
        try {
            Sidecar sidecar = new Sidecar(props, consumer);
            sidecar.start();
        } catch (Exception e) {
            System.out.println(e);
            System.exit(1);
        }
    }
}
