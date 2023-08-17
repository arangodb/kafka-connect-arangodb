package com.arangodb.kafka.target.resilience;

import com.arangodb.kafka.target.write.BaseWriteTarget;

import java.util.Map;

import static com.arangodb.kafka.config.ArangoSinkConfig.*;

public class RetryTarget extends BaseWriteTarget {

    public RetryTarget(String name) {
        super(name);
    }

    @Override
    public Map<String, String> getConfig() {
        Map<String, String> cfg = super.getConfig();
        cfg.put(CONNECTION_PROTOCOL, Protocol.HTTP11.toString());
        cfg.put(MAX_RETRIES, "20");
        cfg.put(RETRY_BACKOFF_MS, "1");
        cfg.put(INSERT_TIMEOUT, "200");
        cfg.put(INSERT_OVERWRITE_MODE, OverwriteMode.REPLACE.toString());
        return cfg;
    }
}
