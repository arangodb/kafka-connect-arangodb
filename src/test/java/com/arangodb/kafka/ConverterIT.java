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
import com.arangodb.kafka.deployment.KafkaConnectDeployment;
import com.arangodb.kafka.deployment.KafkaConnectOperations;
import com.arangodb.kafka.target.Connector;
import com.arangodb.kafka.target.Producer;
import com.arangodb.kafka.utils.KafkaTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;


class ConverterIT {
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

    @Timeout(30)
    @KafkaTest
    void testConversion(ArangoCollection col, Connector connector, Producer producer) {
        String name = connector.getName();

        connectClient.createConnector(connector.getConfig());
        assertThat(connectClient.getConnectors()).contains(name);

        assertThat(col.count().getCount()).isEqualTo(0L);

        for (int i = 0; i < 10; i++) {
            Map<String, Object> data = new HashMap<>();
            data.put("foo", "bar-" + i);
            producer.produce(null, data);
        }

        await("Request received by ADB")
                .atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(500))
                .until(() -> col.count().getCount() >= 10L);

        Iterable<BaseDocument> docs = col.db().query("FOR d IN @@col RETURN d", BaseDocument.class, Collections.singletonMap("@col", name));
        assertThat(docs).allSatisfy(doc -> {
            assertThat(doc.getKey()).startsWith(name);
            assertThat(doc.getAttribute("foo")).asString().startsWith("bar");
        });

        connectClient.deleteConnector(name);
        assertThat(connectClient.getConnectors()).doesNotContain(name);
    }

    @Timeout(30)
    @KafkaTest
    void testConversionWithKeyData(ArangoCollection col, Connector connector, Producer producer) {
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

    @Timeout(30)
    @KafkaTest
    void testConversionWithRecordId(ArangoCollection col, Connector connector, Producer producer) {
        String name = connector.getName();

        connectClient.createConnector(connector.getConfig());
        assertThat(connectClient.getConnectors()).contains(name);

        assertThat(col.count().getCount()).isEqualTo(0L);

        for (int i = 0; i < 10; i++) {
            String key = "id-" + i;
            Map<String, Object> data = new HashMap<>();
            data.put("foo", "bar-" + i);
            producer.produce(key, data);
        }

        await("Request received by ADB")
                .atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(500))
                .until(() -> col.count().getCount() >= 10L);

        BaseDocument doc0 = col.getDocument("id-0", BaseDocument.class);
        assertThat(doc0.getAttribute("foo")).isEqualTo("bar-0");

        connectClient.deleteConnector(name);
        assertThat(connectClient.getConnectors()).doesNotContain(name);
    }

}
