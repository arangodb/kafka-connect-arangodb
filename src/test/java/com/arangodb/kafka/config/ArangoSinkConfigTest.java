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
        baseProps.put(ArangoSinkConfig.CONNECTION_ENDPOINTS, "adb:8529");
        baseProps.put(ArangoSinkConfig.CONNECTION_COLLECTION, "collection");
    }

    @Test
    void defaults() {
        ArangoSinkConfig config = new ArangoSinkConfig(baseProps);
        assertThat(config.getString(ArangoSinkConfig.CONNECTION_DATABASE)).isEqualTo("_system");
        assertThat(config.getString(ArangoSinkConfig.CONNECTION_USER)).isEqualTo("root");
        assertThat(config.getString(ArangoSinkConfig.CONNECTION_PROTOCOL)).isEqualTo("HTTP2");
        assertThat(config.getString(ArangoSinkConfig.CONNECTION_CONTENT_TYPE)).isEqualTo("JSON");
        assertThat(config.getPassword(ArangoSinkConfig.CONNECTION_PASSWORD)).isNull();
        assertThat(config.getBoolean(ArangoSinkConfig.CONNECTION_SSL_ENABLED)).isFalse();
        assertThat(config.getBoolean(ArangoSinkConfig.CONNECTION_SSL_HOSTNAME_VERIFICATION)).isTrue();
        assertThat(config.getString(ArangoSinkConfig.INSERT_OVERWRITE_MODE))
                .isEqualTo(ArangoSinkConfig.OverwriteMode.CONFLICT.toString());
        assertThat(config.getBoolean(ArangoSinkConfig.INSERT_MERGE_OBJECTS)).isTrue();
        assertThat(config.getBatchSize()).isEqualTo(3_000);
        assertThat(config.getBoolean(ArangoSinkConfig.DELETE_ENABLED)).isFalse();
        assertThat(config.getMaxRetries()).isEqualTo(10);
        assertThat(config.getRetryBackoffMs()).isEqualTo(3000);
        assertThat(config.isAcquireHostListEnabled()).isFalse();
        assertThat(config.getAcquireHostIntervalMs()).isEqualTo(60_000);
        assertThat(config.getRebalanceIntervalMs()).isEqualTo(30 * 60 * 1_000);
        assertThat(config.getTolerateDataErrors()).isFalse();
        assertThat(config.getLogDataErrors()).isFalse();
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
