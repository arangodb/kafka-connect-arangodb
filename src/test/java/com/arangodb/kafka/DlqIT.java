package com.arangodb.kafka;

import com.arangodb.ArangoCollection;
import com.arangodb.kafka.target.Producer;
import com.arangodb.kafka.target.write.DlqTarget;
import com.arangodb.kafka.utils.KafkaTest;

import static com.arangodb.kafka.utils.Utils.awaitCount;
import static com.arangodb.kafka.utils.Utils.map;
import static org.assertj.core.api.Assertions.assertThat;

public class DlqIT {
    @KafkaTest(DlqTarget.class)
    void test(ArangoCollection col, Producer producer) {
        producer.produce("invalid#key", map());
        producer.produce("flush", map());
        awaitCount(col, 1);
        assertThat(col.documentExists("flush")).isTrue();
    }
}
