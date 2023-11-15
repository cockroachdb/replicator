package com.cockroachlabs.cdc;

import org.junit.Test;
import java.io.StringWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import java.io.IOException;
import java.io.File;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine.Offsets;
import io.debezium.engine.DebeziumEngine.RecordCommitter;

import static org.assertj.core.api.Assertions.*;

public class CrdbCdcChangeConsumerTest {
    private ObjectMapper objectMapper = new ObjectMapper();

    private class Event implements ChangeEvent<String, String> {
        String key;
        String value;

        Event(String key, String value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String destination() {
            return null;
        }

        @Override
        public String key() {
            return key;
        }

        @Override
        public String value() {
            return value;
        }
    }

    private class Committer implements RecordCommitter<ChangeEvent<String, String>> {

        protected ChangeEvent<String, String> last = null;
        protected Boolean finished = false;

        @Override
        public void markProcessed(ChangeEvent<String, String> record) throws InterruptedException {
            last = record;
        }

        @Override
        public void markBatchFinished() throws InterruptedException {
            finished = true;
        }

        @Override
        public void markProcessed(ChangeEvent<String, String> record, Offsets sourceOffsets)
                throws InterruptedException {
            // TODO Auto-generated method stub
            throw new UnsupportedOperationException("Unimplemented method 'markProcessed'");
        }

        @Override
        public Offsets buildOffsets() {
            // TODO Auto-generated method stub
            throw new UnsupportedOperationException("Unimplemented method 'buildOffsets'");
        }

    }

    @Test
    public void testHandleBatch() {
        String[] tests = { "delete", "insert", "update" };
        for (int i = 0; i < tests.length; i++) {
            try {
                String test = tests[i];
                JsonNode expected = getJson(test, "expected");
                CrdbCdcChangeConsumer consumer = new CrdbCdcChangeConsumer();
                StringWriter sw = new StringWriter();
                Committer committer = new Committer();
                consumer.withWriter(sw);
                List<ChangeEvent<String, String>> events = getEvents(test);
                consumer.handleBatch(events, committer);
                assert (sw.toString().equals(expected.toString()));
                assert (committer.finished);
                assert (committer.last.equals(events.get(events.size() - 1)));
            } catch (Exception e) {
                fail("handle batch failed", e);
            }
        }

    }

    private JsonNode getJson(String test, String type) throws IOException {
        URL url = this.getClass().getResource("/cdc/" + test + "/" + type + ".json");
        File f = new File(url.getFile());
        return objectMapper.readTree(f);
    }

    private List<ChangeEvent<String, String>> getEvents(String test)
            throws IOException {
        ArrayList<ChangeEvent<String, String>> res = new ArrayList<ChangeEvent<String, String>>();
        JsonNode events = getJson(test, "events");
        Iterator<JsonNode> itr = events.elements();
        while (itr.hasNext()) {
            JsonNode a = itr.next();
            res.add(
                    new Event(
                            a.get("key").toString(),
                            a.get("value").toString()));
        }
        return res;
    }
}
