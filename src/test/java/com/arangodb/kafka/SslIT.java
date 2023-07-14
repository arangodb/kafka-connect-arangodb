package com.arangodb.kafka;

import com.arangodb.ArangoCollection;
import com.arangodb.entity.BaseDocument;
import com.arangodb.kafka.deployment.KafkaConnectDeployment;
import com.arangodb.kafka.deployment.KafkaConnectOperations;
import com.arangodb.kafka.target.Connector;
import com.arangodb.kafka.target.Producer;
import com.arangodb.kafka.target.Target;
import com.arangodb.kafka.utils.KafkaTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@EnabledIfSystemProperty(named = "SslTest", matches = "true")
class SslIT {

    private static final KafkaConnectDeployment kafkaConnect = KafkaConnectDeployment.getInstance();
    private static KafkaConnectOperations connectClient;

    @BeforeAll
    static void setUpAll() {
        kafkaConnect.start();
        connectClient = kafkaConnect.client();
    }

    @AfterAll
    static void tearDownAll() {
        kafkaConnect.stop();
    }

    @KafkaTest(Target.SSL)
    void foo(ArangoCollection col, Connector connector, Producer producer) {
        String name = connector.getName();

        connectClient.createConnector(connector.getConfig());
        assertThat(connectClient.getConnectors()).contains(name);

        assertThat(col.count().getCount()).isEqualTo(0L);

        for (int i = 0; i < 10; i++) {
            Map<String, Object> data = new HashMap<>();
            data.put("_key", "k-" + i);
            data.put("foo", "bar-" + i);
            producer.produce(null, data);
        }

        await("Request received by ADB")
                .atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(500))
                .until(() -> col.count().getCount() >= 10L);

        BaseDocument doc0 = col.getDocument("k-0", BaseDocument.class);
        assertThat(doc0.getAttribute("foo")).isEqualTo("bar-0");

        connectClient.deleteConnector(name);
        assertThat(connectClient.getConnectors()).doesNotContain(name);
    }

}
