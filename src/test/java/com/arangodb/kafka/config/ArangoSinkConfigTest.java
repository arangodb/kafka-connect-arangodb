package com.arangodb.kafka.config;

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
        assertThat(config.getString(ArangoSinkConfig.CONNECTION_DATABASE)).isEqualTo("_system");
        assertThat(config.getString(ArangoSinkConfig.CONNECTION_USER)).isEqualTo("root");
        assertThat(config.getString(ArangoSinkConfig.CONNECTION_PROTOCOL)).isEqualTo("HTTP2");
        assertThat(config.getString(ArangoSinkConfig.CONNECTION_CONTENT_TYPE)).isEqualTo("JSON");
        assertThat(config.getPassword(ArangoSinkConfig.CONNECTION_PASSWORD)).isNull();
        assertThat(config.getBoolean(ArangoSinkConfig.CONNECTION_SSL_ENABLED)).isFalse();
        assertThat(config.getBoolean(ArangoSinkConfig.CONNECTION_SSL_HOSTNAME_VERIFICATION)).isTrue();
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

    @Test
    void invalidSslConfig() {
        HashMap<String, String> props = new HashMap<>(baseProps);
        props.put(ArangoSinkConfig.CONNECTION_SSL_CERT_VALUE, "abcde");
        props.put(ArangoSinkConfig.CONNECTION_SSL_TRUSTSTORE_LOCATION, "/tmp");
        Throwable thrown = catchThrowable(() -> new ArangoSinkConfig(props));
        assertThat(thrown)
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining(ArangoSinkConfig.CONNECTION_SSL_CERT_VALUE)
                .hasMessageContaining(ArangoSinkConfig.CONNECTION_SSL_TRUSTSTORE_LOCATION);
    }

}
