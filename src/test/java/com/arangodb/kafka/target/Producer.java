package com.arangodb.kafka.target;

import java.util.Map;

public interface Producer {
    void produce(String key, Map<String, Object> value);
}
