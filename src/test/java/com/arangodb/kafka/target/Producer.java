package com.arangodb.kafka.target;

import java.util.Map;
import java.util.stream.Stream;

public interface Producer {
    default void produce(Stream<Map.Entry<Object, Map<String, Object>>> data) {
        data.forEach(it -> produce(it.getKey(), it.getValue()));
    }

    void produce(Object key, Map<String, Object> value);
    default void produce(Map<String, Object> value) {
        produce(null, value);
    }

    String getTopicName();
}
