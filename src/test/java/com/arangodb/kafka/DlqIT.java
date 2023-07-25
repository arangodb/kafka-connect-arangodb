package com.arangodb.kafka;

import com.arangodb.ArangoCollection;
import com.arangodb.kafka.target.Producer;
import com.arangodb.kafka.target.write.BaseWriteTarget;
import com.arangodb.kafka.utils.KafkaTest;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

import static com.arangodb.kafka.utils.KafkaUtils.extractHeaders;
import static com.arangodb.kafka.utils.Utils.*;
import static org.assertj.core.api.Assertions.assertThat;

public class DlqIT {
    @KafkaTest(BaseWriteTarget.class)
    void illegalKey(ArangoCollection col, Producer producer, Map<String, ConsumerRecord<String, String>> dlq) {
        producer.produce("illegal#key", map());
        producer.produce("flush", map());
        awaitCount(col, 1);
        assertThat(col.documentExists("flush")).isTrue();

        awaitDlq(dlq, 1);
        ConsumerRecord<String, String> dlqMsg = dlq.get("illegal#key");
        assertThat(dlqMsg).isNotNull();
        Map<String, String> headers = extractHeaders(dlqMsg);
        assertThat(headers)
                .containsEntry("__connect.errors.exception.class.name", "org.apache.kafka.connect.errors.ConnectException")
                .containsEntry("__connect.errors.exception.message", "com.arangodb.ArangoDBException: Response: 400, Error: 1221 - illegal document key");
    }
}
