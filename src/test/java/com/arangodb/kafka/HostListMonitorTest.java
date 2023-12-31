package com.arangodb.kafka;

import com.arangodb.ArangoDB;
import com.arangodb.Response;
import com.arangodb.config.HostDescription;
import com.arangodb.kafka.config.ArangoSinkConfig;
import com.arangodb.kafka.utils.MockTest;
import com.arangodb.kafka.utils.Utils;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Spy;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.arangodb.kafka.utils.Utils.map;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@MockTest
public class HostListMonitorTest {
    private Utils.FluentMap<String, Object> config() {
        return map()
                .add(ArangoSinkConfig.CONNECTION_ENDPOINTS, "a:1")
                .add(ArangoSinkConfig.CONNECTION_COLLECTION, "HostListMonitorTest")
                .add(ArangoSinkConfig.CONNECTION_ACQUIRE_HOST_LIST_ENABLED, "true")
                .add(ArangoSinkConfig.CONNECTION_ACQUIRE_HOST_LIST_INTERVAL_MS, "200");
    }

    @Mock
    ArangoDB adb;

    @Mock
    ConnectorContext context;

    @Spy
    ArangoSinkConfig sinkConfig = new ArangoSinkConfig(config());

    @Test
    void acquireHostList() throws InterruptedException {
        doReturn(adb).when(sinkConfig).createMonitorClient();

        HostListMonitor monitor = new HostListMonitor(sinkConfig, context);
        assertThat(monitor.getEndpoints())
                .hasSize(1)
                .contains(new HostDescription("a", 1));

        ObjectNode resp1 = JsonNodeFactory.instance.objectNode()
                .set("endpoints", JsonNodeFactory.instance.arrayNode()
                        .add(JsonNodeFactory.instance.objectNode().put("endpoint", "tcp://b:2"))
                        .add(JsonNodeFactory.instance.objectNode().put("endpoint", "tcp://c:3"))
                );
        when(adb.execute(any(), any()))
                .thenReturn(new Response<>(200, Collections.emptyMap(), resp1));
        monitor.start();

        assertThat(monitor.getEndpoints())
                .hasSize(2)
                .contains(new HostDescription("b", 2))
                .contains(new HostDescription("c", 3));

        ObjectNode resp2 = JsonNodeFactory.instance.objectNode()
                .set("endpoints", JsonNodeFactory.instance.arrayNode()
                        .add(JsonNodeFactory.instance.objectNode().put("endpoint", "tcp://d:4"))
                        .add(JsonNodeFactory.instance.objectNode().put("endpoint", "tcp://e:5"))
                );
        when(adb.execute(any(), any()))
                .thenReturn(new Response<>(200, Collections.emptyMap(), resp2));

        Thread.sleep(250);
        assertThat(monitor.getEndpoints())
                .hasSize(2)
                .contains(new HostDescription("d", 4))
                .contains(new HostDescription("e", 5));
        verify(context, times(1)).requestTaskReconfiguration();

        ObjectNode resp3 = JsonNodeFactory.instance.objectNode();
        when(adb.execute(any(), any()))
                .thenReturn(new Response<>(200, Collections.emptyMap(), resp3));

        Thread.sleep(250);
        assertThat(monitor.getEndpoints())
                .hasSize(2)
                .contains(new HostDescription("d", 4))
                .contains(new HostDescription("e", 5));

        monitor.stop();
        verify(adb, times(1)).shutdown();
    }

    @Test
    void acquireHostListFalse() {
        ArangoSinkConfig cfg = new ArangoSinkConfig(config()
                .add(ArangoSinkConfig.CONNECTION_ACQUIRE_HOST_LIST_ENABLED, "false"));

        HostListMonitor monitor = new HostListMonitor(cfg, context);
        monitor.start();
        assertThat(monitor.getEndpoints()).containsExactly(new HostDescription("a", 1));
        monitor.stop();
    }

    @Test
    void rebalance() throws InterruptedException {
        String epList = IntStream.range(0, 10)
                .mapToObj(i -> "host:" + i)
                .collect(Collectors.joining(","));
        ArangoSinkConfig cfg = new ArangoSinkConfig(config()
                .add(ArangoSinkConfig.CONNECTION_ACQUIRE_HOST_LIST_ENABLED, "false")
                .add(ArangoSinkConfig.CONNECTION_ENDPOINTS, epList)
                .add(ArangoSinkConfig.CONNECTION_REBALANCE_INTERVAL_MS, "200")
        );

        HostListMonitor monitor = new HostListMonitor(cfg, context);
        monitor.start();
        Thread.sleep(250);
        monitor.stop();
        verify(context, times(1)).requestTaskReconfiguration();

        List<HostDescription> endpoints = IntStream.range(0, 10)
                .mapToObj(i -> new HostDescription("host", i))
                .collect(Collectors.toList());
        assertThat(monitor.getEndpoints())
                .containsExactlyInAnyOrderElementsOf(endpoints)
                .isNotEqualTo(endpoints);
    }

}
