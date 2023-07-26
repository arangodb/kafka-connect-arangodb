package com.arangodb.kafka.target.dlq;

import com.arangodb.kafka.config.ArangoSinkConfig;
import com.arangodb.kafka.target.write.BaseWriteTarget;

import java.util.Map;

public class DlqTarget extends BaseWriteTarget {
    public DlqTarget(String name) {
        super(name);
    }

    @Override
    public boolean supportsDLQ() {
        return true;
    }

    @Override
    public Map<String, String> getConfig() {
        Map<String, String> cfg = super.getConfig();
        cfg.put(ArangoSinkConfig.MAX_RETRIES, "3");
        cfg.put(ArangoSinkConfig.RETRY_BACKOFF_MS, "1000");
        return cfg;
    }
}
