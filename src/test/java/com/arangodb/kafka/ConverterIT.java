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
import com.arangodb.config.HostDescription;
import com.arangodb.entity.BaseDocument;
import com.arangodb.kafka.utils.AvroTarget;
import com.arangodb.kafka.utils.JsonTarget;
import com.arangodb.kafka.utils.StringTarget;
import com.arangodb.kafka.utils.TestTarget;
import deployment.ArangoDbDeployment;
import deployment.KafkaConnectDeployment;
import deployment.KafkaConnectOperations;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static com.arangodb.kafka.utils.Config.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;


public class ConverterIT {
    private static final KafkaConnectDeployment kafkaConnect = KafkaConnectDeployment.getInstance();
    private static KafkaConnectOperations connectClient;
    private ArangoCollection col;
    private AdminClient adminClient;

    public static Stream<Arguments> targets() {
        return Stream.of(
                Arguments.of(new JsonTarget()),
                Arguments.of(new AvroTarget()),
                Arguments.of(new StringTarget())
        );
    }

    @BeforeAll
    static void setUpAll() {
        kafkaConnect.start();
        connectClient = kafkaConnect.client();
    }

    @AfterAll
    static void tearDownAll() {
        kafkaConnect.stop();
    }

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException {
        HostDescription adbHost = ArangoDbDeployment.getHost();
        col = new ArangoDB.Builder()
                .host(adbHost.getHost(), adbHost.getPort())
                .password("test")
                .build()
                .db("_system")
                .collection(COLLECTION_NAME);
        if (col.exists()) {
            col.drop();
        }
        col.create();

        Properties adminClientConfig = new Properties();
        adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConnect.getBootstrapServers());
        adminClient = AdminClient.create(adminClientConfig);
        Set<String> topics = adminClient.listTopics().names().get();
        if (topics.contains(TOPIC_NAME)) {
            adminClient.deleteTopics(Collections.singletonList(TOPIC_NAME)).all().get();
        }
        adminClient.createTopics(Collections.singletonList(new NewTopic(TOPIC_NAME, 2, (short) 1))).all().get();
    }

    @AfterEach
    void tearDown() {
        adminClient.close();
    }

    @Timeout(30)
    @ParameterizedTest
    @MethodSource("targets")
    void testBasicDelivery(TestTarget target) {
        connectClient.createConnector(target.config());
        assertThat(connectClient.getConnectors()).contains(CONNECTOR_NAME);

        assertThat(col.count().getCount()).isEqualTo(0L);

        for (int i = 0; i < 10; i++) {
            Map<String, Object> data = new HashMap<>();
            data.put("foo", "bar-" + i);
            target.produce(null, data);
        }
        target.flush();

        await("Request received by ADB")
                .atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(500))
                .until(() -> col.count().getCount() >= 10L);

        Iterable<BaseDocument> docs = col.db().query("FOR d IN @@col RETURN d", BaseDocument.class, Collections.singletonMap("@col", COLLECTION_NAME));
        assertThat(docs).allSatisfy(doc -> {
            assertThat(doc.getKey()).startsWith(TOPIC_NAME);
            assertThat(doc.getAttribute("foo")).asString().startsWith("bar");
        });

        connectClient.deleteConnector(CONNECTOR_NAME);
        assertThat(connectClient.getConnectors()).doesNotContain(CONNECTOR_NAME);
        target.close();
    }

    @Timeout(30)
    @ParameterizedTest
    @MethodSource("targets")
    void testBasicDeliveryWithKeyData(TestTarget target) {
        connectClient.createConnector(target.config());
        assertThat(connectClient.getConnectors()).contains(CONNECTOR_NAME);

        assertThat(col.count().getCount()).isEqualTo(0L);

        for (int i = 0; i < 10; i++) {
            Map<String, Object> data = new HashMap<>();
            data.put("_key", "k-" + i);
            data.put("foo", "bar-" + i);
            target.produce(null, data);
        }
        target.flush();

        await("Request received by ADB")
                .atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(500))
                .until(() -> col.count().getCount() >= 10L);

        BaseDocument doc0 = col.getDocument("k-0", BaseDocument.class);
        assertThat(doc0.getAttribute("foo")).isEqualTo("bar-0");

        connectClient.deleteConnector(CONNECTOR_NAME);
        assertThat(connectClient.getConnectors()).doesNotContain(CONNECTOR_NAME);
        target.close();
    }

    @Timeout(30)
    @ParameterizedTest
    @MethodSource("targets")
    void testBasicDeliveryWithRecordId(TestTarget target) {
        connectClient.createConnector(target.config());
        assertThat(connectClient.getConnectors()).contains(CONNECTOR_NAME);

        assertThat(col.count().getCount()).isEqualTo(0L);

        for (int i = 0; i < 10; i++) {
            String key = "id-" + i;
            Map<String, Object> data = new HashMap<>();
            data.put("foo", "bar-" + i);
            target.produce(key, data);
        }
        target.flush();

        await("Request received by ADB")
                .atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(500))
                .until(() -> col.count().getCount() >= 10L);

        BaseDocument doc0 = col.getDocument("id-0", BaseDocument.class);
        assertThat(doc0.getAttribute("foo")).isEqualTo("bar-0");

        connectClient.deleteConnector(CONNECTOR_NAME);
        assertThat(connectClient.getConnectors()).doesNotContain(CONNECTOR_NAME);
        target.close();
    }

}
