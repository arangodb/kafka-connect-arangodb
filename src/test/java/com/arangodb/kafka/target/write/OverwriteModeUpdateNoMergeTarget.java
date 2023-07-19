package com.arangodb.kafka.target.write;

import com.arangodb.kafka.config.ArangoSinkConfig;
import com.arangodb.kafka.target.converter.JsonTarget;

import java.util.Map;

public class OverwriteModeUpdateNoMergeTarget extends JsonTarget {

    public OverwriteModeUpdateNoMergeTarget(String name) {
        super(name);
    }

    @Override
    public Map<String, String> getConfig() {
        Map<String, String> cfg = super.getConfig();
        cfg.put(ArangoSinkConfig.INSERT_OVERWRITE_MODE, ArangoSinkConfig.OverwriteMode.UPDATE.toString());
        cfg.put(ArangoSinkConfig.INSERT_MERGE_OBJECTS, "false");
        return cfg;
    }

}
