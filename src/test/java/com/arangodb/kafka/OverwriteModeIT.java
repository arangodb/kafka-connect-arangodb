/*
 * Copyright 2023 ArangoDB GmbH, Cologne, Germany
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright holder is ArangoDB GmbH, Cologne, Germany
 */

package com.arangodb.kafka;

import com.arangodb.ArangoCollection;
import com.arangodb.entity.BaseDocument;
import com.arangodb.kafka.target.Producer;
import com.arangodb.kafka.target.write.*;
import com.arangodb.kafka.utils.KafkaTest;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

import static com.arangodb.kafka.utils.KafkaUtils.extractHeaders;
import static com.arangodb.kafka.utils.Utils.*;
import static org.assertj.core.api.Assertions.assertThat;


class OverwriteModeIT {

    @KafkaTest(OverwriteModeConflictTarget.class)
    void testWriteConflict(ArangoCollection col, Producer producer, Map<String, ConsumerRecord<String, String>> dlq) {
        producer.produce("key", map().add("value", "foo"));
        producer.produce("key", map().add("value", "bar"));
        producer.produce("flush", map());

        awaitCount(col, 2);
        assertThat(col.documentExists("key")).isTrue();
        assertThat(col.documentExists("flush")).isTrue();

        awaitDlq(dlq, 1);
        ConsumerRecord<String, String> dlqMsg = dlq.get("key");
        assertThat(dlqMsg).isNotNull();
        Map<String, String> headers = extractHeaders(dlqMsg);
        assertThat(headers)
                .containsEntry("__connect.errors.exception.class.name", "org.apache.kafka.connect.errors.DataException")
                .hasEntrySatisfying("__connect.errors.exception.message", v ->
                        assertThat(v).contains("Error: 1210 - unique constraint violated"));
    }

    @KafkaTest(OverwriteModeIgnoreTarget.class)
    void testWriteIgnore(ArangoCollection col, Producer producer) {
        producer.produce("key", map().add("value", "foo"));
        producer.produce("key", map().add("value", "bar"));
        producer.produce("flush", map());
        awaitCount(col, 2);

        BaseDocument doc = col.getDocument("key", BaseDocument.class);
        assertThat(doc.getAttribute("value")).isEqualTo("foo");
    }

    @KafkaTest(OverwriteModeReplaceTarget.class)
    void testWriteReplace(ArangoCollection col, Producer producer) {
        producer.produce("key", map().add("value", "foo"));
        producer.produce("key", map().add("value", "bar"));
        producer.produce("flush", map());
        awaitCount(col, 2);

        BaseDocument doc = col.getDocument("key", BaseDocument.class);
        assertThat(doc.getAttribute("value")).isEqualTo("bar");
    }

    @KafkaTest(OverwriteModeUpdateTarget.class)
    void testWriteUpdate(ArangoCollection col, Producer producer) {
        producer.produce("key", map()
                .add("value", "foo")
                .add("fooField", "foo")
                .add("nested", map()
                        .add("fooField", "foo")
                )
        );
        producer.produce("key", map()
                .add("value", "bar")
                .add("barField", "bar")
                .add("nested", map()
                        .add("barField", "bar")
                )
        );
        producer.produce("flush", map());
        awaitCount(col, 2);

        ObjectNode doc = col.getDocument("key", ObjectNode.class);
        assertThat(doc.get("value").textValue()).isEqualTo("bar");
        assertThat(doc.get("fooField").textValue()).isEqualTo("foo");
        assertThat(doc.get("barField").textValue()).isEqualTo("bar");
        assertThat(doc.get("nested").get("fooField").textValue()).isEqualTo("foo");
        assertThat(doc.get("nested").get("barField").textValue()).isEqualTo("bar");
    }

    @KafkaTest(OverwriteModeUpdateNoMergeTarget.class)
    void testWriteUpdateNoMerge(ArangoCollection col, Producer producer) {
        producer.produce("key", map()
                .add("value", "foo")
                .add("fooField", "foo")
                .add("nested", map()
                        .add("fooField", "foo")
                )
        );
        producer.produce("key", map()
                .add("value", "bar")
                .add("barField", "bar")
                .add("nested", map()
                        .add("barField", "bar")
                )
        );
        producer.produce("flush", map());
        awaitCount(col, 2);

        ObjectNode doc = col.getDocument("key", ObjectNode.class);
        assertThat(doc.get("value").textValue()).isEqualTo("bar");
        assertThat(doc.get("fooField").textValue()).isEqualTo("foo");
        assertThat(doc.get("barField").textValue()).isEqualTo("bar");
        assertThat(doc.get("nested").has("fooField")).isFalse();
        assertThat(doc.get("nested").get("barField").textValue()).isEqualTo("bar");
    }

}
