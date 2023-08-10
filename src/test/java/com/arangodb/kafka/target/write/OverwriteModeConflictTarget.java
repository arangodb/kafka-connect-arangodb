package com.arangodb.kafka.target.write;

import java.util.Map;

import static com.arangodb.kafka.config.ArangoSinkConfig.*;

public class OverwriteModeConflictTarget extends BaseWriteTarget {

    public OverwriteModeConflictTarget(String name) {
        super(name);
    }

    @Override
    public Map<String, String> getConfig() {
        Map<String, String> cfg = super.getConfig();
        cfg.put(INSERT_OVERWRITE_MODE, OverwriteMode.CONFLICT.toString());
        cfg.put(DATA_ERRORS_TOLERANCE, DataErrorsTolerance.ALL.toString());
        return cfg;
    }

    @Override
    public boolean supportsDLQ() {
        return true;
    }
}
