package com.arangodb.kafka.target.converter;

import com.arangodb.kafka.target.TargetHolder;
import com.arangodb.kafka.target.TestTarget;

public enum ConvertTargets implements TargetHolder {
    JsonTarget(JsonTarget.class),
    AvroTarget(AvroTarget.class),
    StringTarget(StringTarget.class),
    JsonWithSchemaTarget(JsonWithSchemaTarget.class);

    private final Class<? extends TestTarget> clazz;

    ConvertTargets(final Class<? extends TestTarget> clazz) {
        this.clazz = clazz;
    }

    @Override
    public Class<? extends TestTarget> getClazz() {
        return clazz;
    }
}
