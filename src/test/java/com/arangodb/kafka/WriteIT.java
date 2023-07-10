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
import com.arangodb.ArangoDB;
import com.arangodb.entity.BaseDocument;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import deployment.KafkaConnectDeployment;
import deployment.KafkaConnectOperations;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
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
    private static final String SCHEMA_REGISTRY_URL = "http://172.28.11.21:8081";
    private static final String CONNECTOR_NAME = "my-connector";
    private static final String CONNECTOR_CLASS = ArangoSinkConnector.class.getName();
    private static KafkaConnectDeployment kafkaConnect;
    private static KafkaConnectOperations connectClient;
    private static final Schema VALUE_RECORD_SCHEMA = new Schema.Parser()
            .parse("{"
                    + "  \"type\":\"record\","
                    + "  \"name\":\"record\","
                    + "  \"fields\": ["
                    + "    {\"name\":\"_key\",\"type\":\"string\"}, "
                    + "    {\"name\":\"foo\",\"type\":\"string\"}"
                    + "  ]"
                    + "}");

    private String topicName;
    private ArangoCollection col;
    private AdminClient adminClient;
    private KafkaProducer<JsonNode, JsonNode> jsonProducer;
    private KafkaProducer<String, GenericRecord> avroProducer;
    private KafkaProducer<String, String> stringProducer;


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

        Map<String, Object> jsonProducerProps = new HashMap<>();
        jsonProducerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnect.getBootstrapServers());
        jsonProducerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonSerializer");
        jsonProducerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonSerializer");
        jsonProducerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        jsonProducer = new KafkaProducer<>(jsonProducerProps);

        Map<String, Object> avroProducerProps = new HashMap<>();
        avroProducerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnect.getBootstrapServers());
        avroProducerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        avroProducerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        avroProducerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        avroProducerProps.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        avroProducer = new KafkaProducer<>(avroProducerProps);

        Map<String, Object> stringProducerProps = new HashMap<>();
        stringProducerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnect.getBootstrapServers());
        stringProducerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        stringProducerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        stringProducerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        stringProducer = new KafkaProducer<>(stringProducerProps);
    }

    @AfterEach
    void tearDown() {
        adminClient.close();
        jsonProducer.close();
    }

    @Test
    @Timeout(30)
    void testBasicDeliveryJson() throws ExecutionException, InterruptedException {
        Map<String, String> config = new HashMap<>();
        config.put("name", CONNECTOR_NAME);
        config.put("connector.class", CONNECTOR_CLASS);
        config.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        config.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        config.put("key.converter.schemas.enable", "false");
        config.put("value.converter.schemas.enable", "false");
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
            jsonProducer.send(new ProducerRecord<>(topicName,
                    JsonNodeFactory.instance.objectNode().put("id", "foo-" + i),
                    JsonNodeFactory.instance.objectNode()
                            .put("_key", "k-" + i)
                            .put("foo", "bar-" + i)
            )).get();
        }
        jsonProducer.flush();

        await("Request received by ADB")
                .atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(100))
                .until(() -> col.count().getCount() >= 1_000L);

        BaseDocument doc0 = col.getDocument("k-0", BaseDocument.class);
        assertThat(doc0.getAttribute("foo")).isEqualTo("bar-0");

        connectClient.deleteConnector(CONNECTOR_NAME);
        assertThat(connectClient.getConnectors()).doesNotContain(CONNECTOR_NAME);
    }

    @Test
    @Timeout(30)
    void testBasicDeliveryAvro() throws ExecutionException, InterruptedException {
        Map<String, String> config = new HashMap<>();
        config.put("name", CONNECTOR_NAME);
        config.put("connector.class", CONNECTOR_CLASS);
        config.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        config.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("value.converter.schema.registry.url", SCHEMA_REGISTRY_URL);
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
            GenericData.Record valueRecord = new GenericData.Record(VALUE_RECORD_SCHEMA);
            valueRecord.put("_key", "k-" + i);
            valueRecord.put("foo", "bar-" + i);

            avroProducer.send(new ProducerRecord<>(topicName,
                    "id-" + i,
                    valueRecord
            )).get();
        }
        avroProducer.flush();

        await("Request received by ADB")
                .atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(100))
                .until(() -> col.count().getCount() >= 1_000L);

        BaseDocument doc0 = col.getDocument("k-0", BaseDocument.class);
        assertThat(doc0.getAttribute("foo")).isEqualTo("bar-0");

        connectClient.deleteConnector(CONNECTOR_NAME);
        assertThat(connectClient.getConnectors()).doesNotContain(CONNECTOR_NAME);
    }

    @Test
    @Timeout(30)
    void testBasicDeliveryString() throws ExecutionException, InterruptedException {
        Map<String, String> config = new HashMap<>();
        config.put("name", CONNECTOR_NAME);
        config.put("connector.class", CONNECTOR_CLASS);
        config.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        config.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        config.put("key.converter.schemas.enable", "false");
        config.put("value.converter.schemas.enable", "false");
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
            stringProducer.send(new ProducerRecord<>(topicName,
                    "\"id-" + i + "\"",
                    "{\"_key\":\"k-" + i + "\",\"foo\":\"bar-" + i + "\"}"
            )).get();
        }
        stringProducer.flush();

        await("Request received by ADB")
                .atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(100))
                .until(() -> col.count().getCount() >= 1_000L);

        BaseDocument doc0 = col.getDocument("k-0", BaseDocument.class);
        assertThat(doc0.getAttribute("foo")).isEqualTo("bar-0");

        connectClient.deleteConnector(CONNECTOR_NAME);
        assertThat(connectClient.getConnectors()).doesNotContain(CONNECTOR_NAME);
    }


}
