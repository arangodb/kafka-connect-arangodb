package com.arangodb.kafka;

import com.arangodb.kafka.config.ArangoSinkConfig;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class LoadBalancingTest {
    private Map<String, String> config() {
        Map<String, String> cfg = new HashMap<>();
        cfg.put(ArangoSinkConfig.CONNECTION_ENDPOINTS, "a:1,b:2,c:3,d:4,e:5,f:6");
        cfg.put(ArangoSinkConfig.CONNECTION_COLLECTION, "LoadBalancingTest");
        return cfg;
    }

    @Test
    void rotateHosts() {
        ArangoSinkConnector connector = new ArangoSinkConnector();
        connector.start(config());

        List<Map<String, String>> taskConfigs2 = connector.taskConfigs(2);
        assertThat(taskConfigs2.get(0).get(ArangoSinkConfig.CONNECTION_ENDPOINTS))
                .isEqualTo("d:4,e:5,f:6,a:1,b:2,c:3");
        assertThat(taskConfigs2.get(1).get(ArangoSinkConfig.CONNECTION_ENDPOINTS))
                .isEqualTo("a:1,b:2,c:3,d:4,e:5,f:6");

        List<Map<String, String>> taskConfigs3 = connector.taskConfigs(3);
        assertThat(taskConfigs3.get(0).get(ArangoSinkConfig.CONNECTION_ENDPOINTS))
                .isEqualTo("e:5,f:6,a:1,b:2,c:3,d:4");
        assertThat(taskConfigs3.get(1).get(ArangoSinkConfig.CONNECTION_ENDPOINTS))
                .isEqualTo("c:3,d:4,e:5,f:6,a:1,b:2");
        assertThat(taskConfigs3.get(2).get(ArangoSinkConfig.CONNECTION_ENDPOINTS))
                .isEqualTo("a:1,b:2,c:3,d:4,e:5,f:6");

        List<Map<String, String>> taskConfigs12 = connector.taskConfigs(12);
        assertThat(taskConfigs12.get(0).get(ArangoSinkConfig.CONNECTION_ENDPOINTS))
                .isEqualTo("f:6,a:1,b:2,c:3,d:4,e:5");
        assertThat(taskConfigs12.get(1).get(ArangoSinkConfig.CONNECTION_ENDPOINTS))
                .isEqualTo("e:5,f:6,a:1,b:2,c:3,d:4");
        assertThat(taskConfigs12.get(2).get(ArangoSinkConfig.CONNECTION_ENDPOINTS))
                .isEqualTo("d:4,e:5,f:6,a:1,b:2,c:3");
        assertThat(taskConfigs12.get(6).get(ArangoSinkConfig.CONNECTION_ENDPOINTS))
                .isEqualTo("f:6,a:1,b:2,c:3,d:4,e:5");
        assertThat(taskConfigs12.get(7).get(ArangoSinkConfig.CONNECTION_ENDPOINTS))
                .isEqualTo("e:5,f:6,a:1,b:2,c:3,d:4");
        assertThat(taskConfigs12.get(8).get(ArangoSinkConfig.CONNECTION_ENDPOINTS))
                .isEqualTo("d:4,e:5,f:6,a:1,b:2,c:3");
    }

}
