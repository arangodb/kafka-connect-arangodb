package com.arangodb.kafka;

import com.arangodb.ArangoCollection;
import com.arangodb.entity.BaseDocument;
import com.arangodb.kafka.deployment.KafkaConnectOperations;
import com.arangodb.kafka.target.Connector;
import com.arangodb.kafka.target.Producer;
import com.arangodb.kafka.target.ssl.SslTargets;
import com.arangodb.kafka.utils.KafkaTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.stream.IntStream;

import static com.arangodb.kafka.utils.Utils.*;
import static org.assertj.core.api.Assertions.assertThat;

@EnabledIfSystemProperty(named = "SslTest", matches = "true")
class SslIT {

    @BeforeEach
    void setup(KafkaConnectOperations connectClient, Connector connector) {
        connectClient.createConnector(connector.getConfig());
    }

    @AfterEach
    void shutdown(KafkaConnectOperations connectClient, Connector connector) {
        connectClient.deleteConnector(connector.getName());
    }

    @KafkaTest(group = SslTargets.class)
    void basicDelivery(ArangoCollection col, Producer producer) {
        assertThat(col.count().getCount()).isEqualTo(0L);

        producer.produce(IntStream.range(0, 10)
                .mapToObj(i -> record("id-" + i, map().add("foo", "bar-" + i))));

        awaitCount(col, 10);

        BaseDocument doc0 = col.getDocument("id-0", BaseDocument.class);
        assertThat(doc0.getAttribute("foo")).isEqualTo("bar-0");
    }

}
