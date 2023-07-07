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

package com.arangodb;

import com.arangodb.entity.BaseDocument;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import deployment.KafkaConnectDeployment;
import deployment.KafkaConnectOperations;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;


public class WriteIT {

    private static final String ADB_HOST = "172.28.0.1";
    private static final int ADB_PORT = 8529;
    private static final String CONNECTOR_NAME = "my-connector";
    private static final String CONNECTOR_CLASS = ArangoSinkConnector.class.getName();
    private static KafkaConnectDeployment kafkaConnect;
    private static KafkaConnectOperations connectClient;

    private String topicName;
    private ArangoCollection col;
    private AdminClient adminClient;
    private KafkaProducer<JsonNode, JsonNode> producer;

    @BeforeAll
    static void setUpAll() {
        kafkaConnect = KafkaConnectDeployment.getInstance();
        kafkaConnect.start();
        connectClient = kafkaConnect.client();
    }

    @AfterAll
    static void tearDownAll() {
        kafkaConnect.stop();
    }

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException {
        String colName = "col-" + UUID.randomUUID();
        col = new ArangoDB.Builder()
                .host(ADB_HOST, ADB_PORT)
                .password("test")
                .build()
                .db("_system")
                .collection(colName);
        if (col.exists()) {
            col.drop();
        }
        col.create();

        topicName = "topic-" + UUID.randomUUID();
        Properties adminClientConfig = new Properties();
        adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnect.getBootstrapServers());
        adminClient = AdminClient.create(adminClientConfig);
        adminClient.createTopics(Collections.singletonList(new NewTopic(topicName, 2, (short) 1))).all().get();

        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnect.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonSerializer");
        producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        producer = new KafkaProducer<>(producerProps);
    }

    @AfterEach
    void tearDown() {
        adminClient.close();
        producer.close();
    }

    @Test
    @Timeout(30)
    void testBasicDelivery() throws ExecutionException, InterruptedException {
        Map<String, String> config = new HashMap<>();
        config.put("name", CONNECTOR_NAME);
        config.put("connector.class", CONNECTOR_CLASS);
        config.put("topics", topicName);
        config.put("tasks.max", "2");
        config.put("arango.host", ADB_HOST);
        config.put("arango.port", String.valueOf(ADB_PORT));
        config.put("arango.user", "root");
        config.put("arango.password", "test");
        config.put("arango.database", "_system");
        config.put("arango.collection", col.name());

        connectClient.createConnector(config);
        assertThat(connectClient.getConnectors()).contains(CONNECTOR_NAME);

        assertThat(col.count().getCount()).isEqualTo(0L);

        for (int i = 0; i < 1_000; i++) {
            producer.send(new ProducerRecord<>(topicName,
                    JsonNodeFactory.instance.objectNode().put("id", "foo-" + i),
                    JsonNodeFactory.instance.objectNode()
                            .put("_key", "k-" + i)
                            .put("foo", "bar-" + i)
            )).get();
        }
        producer.flush();

        await("Request received by ADB")
                .atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(100))
                .until(() -> col.count().getCount() >= 1_000L);

        BaseDocument doc0 = col.getDocument("k-0", BaseDocument.class);
        assertThat(doc0.getAttribute("foo")).isEqualTo("bar-0");

        connectClient.deleteConnector(CONNECTOR_NAME);
        assertThat(connectClient.getConnectors()).doesNotContain(CONNECTOR_NAME);
    }

}
