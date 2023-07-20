package com.arangodb.kafka;

import com.arangodb.ArangoCollection;
import com.arangodb.kafka.target.Producer;
import com.arangodb.kafka.target.ssl.SslTargets;
import com.arangodb.kafka.utils.KafkaTest;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import static com.arangodb.kafka.utils.Utils.awaitCount;
import static com.arangodb.kafka.utils.Utils.map;
import static org.assertj.core.api.Assertions.assertThat;

@EnabledIfSystemProperty(named = "SslTest", matches = "true")
class SslIT {

    @KafkaTest(group = SslTargets.class)
    void basicDelivery(ArangoCollection col, Producer producer) {
        producer.produce("id", map());
        awaitCount(col, 1);
        assertThat(col.documentExists("id")).isTrue();
    }

}
