package com.arangodb.kafka.target.write;

import com.arangodb.kafka.config.ArangoSinkConfig;

import java.util.Map;

public class DeleteTarget extends BaseWriteTarget {

    public DeleteTarget(String name) {
        super(name);
    }

    @Override
    public Map<String, String> getConfig() {
        Map<String, String> cfg = super.getConfig();
        cfg.put(ArangoSinkConfig.DELETE_ENABLED, "true");
        cfg.put(ArangoSinkConfig.INSERT_OVERWRITE_MODE, ArangoSinkConfig.OverwriteMode.REPLACE.toString());
        return cfg;
    }

}
