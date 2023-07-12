package com.arangodb.kafka.config;

import com.arangodb.Protocol;
import com.arangodb.config.HostDescription;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

class ArangoSinkConfigTest {
    private final Map<String, String> baseProps;

    ArangoSinkConfigTest() {
        baseProps = new HashMap<>();
        baseProps.put(ArangoSinkConfig.CONNECTION_ENDPOINTS, "localhost:8529");
        baseProps.put(ArangoSinkConfig.CONNECTION_COLLECTION, "collection");
    }

    @Test
    void connectionDefaults() {
        ArangoSinkConfig config = new ArangoSinkConfig(baseProps);
        assertThat(config.getDatabase()).isEqualTo("_system");
        assertThat(config.getUser()).isEqualTo("root");
        assertThat(config.getProtocol()).isEqualTo(Protocol.HTTP2_JSON);
        assertThat(config.getPassword()).isNull();
    }

    @Test
    void endpoints() {
        HashMap<String, String> props = new HashMap<>(baseProps);
        props.put(ArangoSinkConfig.CONNECTION_ENDPOINTS, "a:1,b:2,c:3");
        ArangoSinkConfig config = new ArangoSinkConfig(props);
        assertThat(config.getEndpoints()).containsExactly(
                new HostDescription("a", 1),
                new HostDescription("b", 2),
                new HostDescription("c", 3)
        );
    }

    @Test
    void user() {
        HashMap<String, String> props = new HashMap<>(baseProps);
        props.put(ArangoSinkConfig.CONNECTION_USER, "jack");
        ArangoSinkConfig config = new ArangoSinkConfig(props);
        assertThat(config.getUser()).isEqualTo("jack");
    }

    @Test
    void password() {
        HashMap<String, String> props = new HashMap<>(baseProps);
        props.put(ArangoSinkConfig.CONNECTION_PASSWORD, "passwd");
        ArangoSinkConfig config = new ArangoSinkConfig(props);
        assertThat(config.getPassword().toString()).doesNotContain("passwd");
        assertThat(config.getPassword().value()).isEqualTo("passwd");
    }

    @Test
    void database() {
        HashMap<String, String> props = new HashMap<>(baseProps);
        props.put(ArangoSinkConfig.CONNECTION_DATABASE, "db");
        ArangoSinkConfig config = new ArangoSinkConfig(props);
        assertThat(config.getDatabase()).isEqualTo("db");
    }

    @Test
    void collection() {
        HashMap<String, String> props = new HashMap<>(baseProps);
        props.put(ArangoSinkConfig.CONNECTION_COLLECTION, "c");
        ArangoSinkConfig config = new ArangoSinkConfig(props);
        assertThat(config.getCollection()).isEqualTo("c");
    }

    @Test
    void protocol() {
        HashMap<String, String> props = new HashMap<>(baseProps);
        props.put(ArangoSinkConfig.CONNECTION_PROTOCOL, ArangoSinkConfig.Protocol.HTTP11.toString());
        props.put(ArangoSinkConfig.CONNECTION_CONTENT_TYPE, ArangoSinkConfig.ContentType.VPACK.toString());
        ArangoSinkConfig config = new ArangoSinkConfig(props);
        assertThat(config.getProtocol()).isEqualTo(Protocol.HTTP_VPACK);
    }

    @Test
    void invalidProtocol() {
        HashMap<String, String> props = new HashMap<>(baseProps);
        props.put(ArangoSinkConfig.CONNECTION_PROTOCOL, "HTTP3");
        Throwable thrown = catchThrowable(() -> new ArangoSinkConfig(props));
        assertThat(thrown)
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(ArangoSinkConfig.CONNECTION_PROTOCOL)
                .hasMessageContaining("HTTP3");
    }

    @Test
    void invalidContentType() {
        HashMap<String, String> props = new HashMap<>(baseProps);
        props.put(ArangoSinkConfig.CONNECTION_CONTENT_TYPE, "AVRO");
        Throwable thrown = catchThrowable(() -> new ArangoSinkConfig(props));
        assertThat(thrown)
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(ArangoSinkConfig.CONNECTION_CONTENT_TYPE)
                .hasMessageContaining("AVRO");
    }

}
