package com.arangodb;

import com.arangodb.entity.BaseDocument;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class ArangoSinkTask extends SinkTask {
    private static final Logger LOG = LoggerFactory.getLogger(ArangoSinkTask.class);
    private String taskId;
    private ArangoCollection col;

    @Override
    public String version() {
        return "1.0.0-SNAPSHOT";
    }

    @Override
    public void start(Map<String, String> props) {
        taskId = props.get("taskId");
        LOG.info("starting task: {}", taskId);
        LOG.info("task config: {}", props);

        col = new ArangoDB.Builder()
                .host(props.get("arango.host"), Integer.parseInt(props.get("arango.port")))
                .password(props.get("arango.password"))
                .build()
                .db(props.get("arango.database"))
                .collection(props.get("arango.collection"));

    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        LOG.info("writing {} record(s)", records.size());
        for (SinkRecord record : records) {
            LOG.info("rcv msg: {}-{}-{}", record.topic(), record.kafkaPartition(), record.kafkaOffset());
            col.insertDocument(new BaseDocument());
        }
        LOG.info(col.db().getVersion().getVersion());
    }

    @Override
    public void stop() {
        LOG.info("stopping task: {}", taskId);
        col.db().arango().shutdown();
    }
}
