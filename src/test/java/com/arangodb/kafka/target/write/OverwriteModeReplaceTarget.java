package com.arangodb.kafka.target.write;

import com.arangodb.kafka.config.ArangoSinkConfig;
import com.arangodb.kafka.target.converter.JsonTarget;

import java.util.Map;

public class OverwriteModeReplaceTarget extends JsonTarget {

    public OverwriteModeReplaceTarget(String name) {
        super(name);
    }

    @Override
    public Map<String, String> getConfig() {
        Map<String, String> cfg = super.getConfig();
        cfg.put(ArangoSinkConfig.INSERT_OVERWRITE_MODE, ArangoSinkConfig.OverwriteMode.REPLACE.toString());
        return cfg;
    }

}
