package com.arangodb.kafka.target.dlq;

import com.arangodb.kafka.target.write.BaseWriteTarget;

import java.util.Map;

import static com.arangodb.kafka.config.ArangoSinkConfig.*;

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
        cfg.put(MAX_RETRIES, "3");
        cfg.put(RETRY_BACKOFF_MS, "1000");
        cfg.put(DATA_ERRORS_TOLERANCE, DataErrorsTolerance.ALL.toString());
        return cfg;
    }
}
