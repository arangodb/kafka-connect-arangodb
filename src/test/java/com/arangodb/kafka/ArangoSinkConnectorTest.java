package com.arangodb.kafka;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ArangoSinkConnectorTest {
    @Test
    void version(){
        String version = new ArangoSinkConnector().version();
        assertThat(version).isEqualTo("1.4.0");
    }
}
