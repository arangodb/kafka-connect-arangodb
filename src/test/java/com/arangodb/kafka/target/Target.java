package com.arangodb.kafka.target;

import java.util.function.Function;

public enum Target {
    JSON(JsonTarget::new),
    AVRO(AvroTarget::new),
    STRING(StringTarget::new),
    JSON_WITH_SCHEMA(JsonWithSchemaTarget::new);

    private final Function<String, TestTarget> constructor;

    Target(Function<String, TestTarget> constructor) {
        this.constructor = constructor;
    }

    public TestTarget create(String name) {
        return constructor.apply(name);
    }
}
