package com.cockroachlabs.cdc;

import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine.ChangeConsumer;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

public abstract class BaseChangeConsumer implements ChangeConsumer<ChangeEvent<String, String>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseChangeConsumer.class);
    private HttpRequest.Builder requestBuilder;
    private Writer out = null;
    HttpClient client = null;

    private static final Long HTTP_TIMEOUT = Integer.toUnsignedLong(60000); // Default to 60s
    private static final int DEFAULT_RETRIES = 5;
    private static final Long RETRY_INTERVAL = Integer.toUnsignedLong(1_000); // Default to 1s

    private Duration timeoutDuration;
    private int maxRetries;
    private Duration retryInterval;

    public BaseChangeConsumer() {
        requestBuilder = null;
        timeoutDuration = Duration.ofMillis(HTTP_TIMEOUT);
        maxRetries = DEFAULT_RETRIES;
        retryInterval = Duration.ofMillis(RETRY_INTERVAL);
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public Duration getRetryInterval() {
        return retryInterval;
    }

    public Duration getTimeoutDuration() {
        return timeoutDuration;
    }

    public void setTimeoutDuration(Duration timeoutDuration) {
        this.timeoutDuration = timeoutDuration;
    }

    public void setMaxRetries(int retries) {
        this.maxRetries = retries;
    }

    public void setRetryInterval(Duration retryInterval) {
        this.retryInterval = retryInterval;
    }

    /**
     * Writes the events to the specified writer.
     *
     * @param w the writer.
     */
    public void withWriter(Writer w) {
        out = w;
    }

    /**
     * Posts the events to replicator at the specified url.
     *
     * @param url the url of replicator service.
     * @throws IOException
     */
    public void withService(String url, Boolean skipVerify) throws GeneralSecurityException {
        requestBuilder = HttpRequest.newBuilder()
                .timeout(timeoutDuration)
                .uri(URI.create(url));
        if (skipVerify) {
            insecureClient();
        } else {
            client = HttpClient.newHttpClient();
        }
    }

    /**
     * Creates a client that allows all certificates, without any verification.
     * To be used only for testing.
     *
     * @throws GeneralSecurityException
     */
    void insecureClient() throws GeneralSecurityException {
        var trustManager = new X509TrustManager() {
            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[] {};
            }

            @Override
            public void checkClientTrusted(X509Certificate[] chain, String authType) {
            }

            @Override
            public void checkServerTrusted(X509Certificate[] chain, String authType) {
            }

        };
        var sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, new TrustManager[] { trustManager }, new SecureRandom());
        HttpClient.Builder builder = HttpClient.newBuilder().sslContext(sslContext);
        client = builder.build();
    }

    protected void post(String body) throws IOException, InterruptedException {
        LOGGER.info("Posting {}", body);
        if (client != null) {
            int retries = 0;
            while (retries < maxRetries) {
                HttpRequest request = requestBuilder
                        .POST(BodyPublishers.ofString(body))
                        .build();
                HttpResponse<String> response = client.send(request,
                        HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() == 200) {
                    break;
                } else {
                    Metronome.sleeper(retryInterval, Clock.SYSTEM).pause();
                }
                retries++;
            }
            if (retries >= maxRetries) {
                throw new DebeziumException("Exceeded maximum number of attempts to publish event");
            }
        }
    }

    protected void write(String buffer) throws IOException {
        if (out != null) {
            out.write(buffer);
            out.flush();
        }
    }

    /**
     * Marks all the given records as processed.
     *
     * @param records   the change events, as serialized key-value pair of JSON
     *                  objects.
     * @param committer the object keeping track of the status.
     * @throws InterruptedException
     */
    protected void markProcessed(List<ChangeEvent<String, String>> records,
            RecordCommitter<ChangeEvent<String, String>> committer) throws InterruptedException {
        for (ChangeEvent<String, String> r : records) {
            try {
                LOGGER.debug("marking committed: " + r.key());
                committer.markProcessed(r);
            } catch (Exception e) {
                throw new DebeziumException("Unable to mark records as processed");
            }
        }
        committer.markBatchFinished();
    }

    public void setRequiredProperties(Properties props) {
        // Only enable schema for keys.
        // In replicator we can inspect the schema for keys
        // to determine the right order to produce the types.Mutation.Key
        props.setProperty("converter.schemas.enable", "false");
        props.setProperty("value.converter.schemas.enable", "false");
        props.setProperty("key.converter.schemas.enable", "true");
        // Setting plugin.path to empty string to silence a startup warning.
        props.setProperty("plugin.path", "");
    }
}
