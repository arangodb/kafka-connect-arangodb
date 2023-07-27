package com.arangodb.kafka.target.write;

import com.arangodb.kafka.config.ArangoSinkConfig;

import java.util.Map;

public class DeleteDisabledTarget extends BaseWriteTarget {

    public DeleteDisabledTarget(String name) {
        super(name);
    }

    @Override
    public Map<String, String> getConfig() {
        Map<String, String> cfg = super.getConfig();
        cfg.put(ArangoSinkConfig.DELETE_ENABLED, "false");
        cfg.put(ArangoSinkConfig.TRANSIENT_ERRORS_TOLERANCE, ArangoSinkConfig.TransientErrorsTolerance.ALL.toString());
        cfg.put(ArangoSinkConfig.MAX_RETRIES, "0");
        return cfg;
    }

    @Override
    public boolean supportsDLQ() {
        return true;
    }
}
