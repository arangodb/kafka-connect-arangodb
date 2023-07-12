package com.arangodb.kafka.target;

import java.util.Map;

public interface Connector {
    String getName();

    Map<String, String> getConfig();
}
