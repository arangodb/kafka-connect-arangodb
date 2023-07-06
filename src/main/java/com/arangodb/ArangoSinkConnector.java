package com.arangodb;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ArangoSinkConnector extends SinkConnector {
    private static final Logger LOG = LoggerFactory.getLogger(SinkConnector.class);
    private Map<String, String> config;

    @Override
    public String version() {
        return "1.0.0-SNAPSHOT";
    }

    @Override
    public void start(Map<String, String> props) {
        LOG.info("starting connector");
        LOG.info("connector config: {}", props);
        this.config = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ArangoSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            HashMap<String, String> cfg = new HashMap<>(config);
            cfg.put("taskId", String.valueOf(i));
            configs.add(cfg);
        }
        return configs;
    }

    @Override
    public void stop() {
        LOG.info("stopping ArangoSinkConnector");
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }
}
