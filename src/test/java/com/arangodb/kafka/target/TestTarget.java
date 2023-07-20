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

package com.arangodb.kafka.target;

import com.arangodb.ArangoCollection;
import com.arangodb.kafka.config.ArangoSinkConfig;
import com.arangodb.kafka.deployment.ArangoDbDeployment;
import com.arangodb.kafka.deployment.KafkaConnectDeployment;
import com.arangodb.kafka.utils.Config;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.ExecutionException;


public abstract class TestTarget implements Connector, Producer, Closeable {
    private final String name;
    private KafkaProducer<Object, Object> producer;
    private ArangoCollection collection;
    private AdminClient adminClient;

    public TestTarget(String name) {
        this.name = name;
    }

    public void init() {
        producer = new KafkaProducer<>(producerConfig());
        collection = createCollection();
        adminClient = createAdminClient();
        try {
            createTopic();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getTopicName() {
        return name;
    }

    public ArangoCollection getCollection() {
        return collection;
    }

    public Object serializeRecordKey(Object key) {
        return key.toString();
    }

    abstract public Object serializeRecordValue(Map<String, Object> data);

    public Map<String, Object> producerConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConnectDeployment.getInstance().getBootstrapServers());
        cfg.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        return cfg;
    }

    @Override
    public Map<String, String> getConfig() {
        Map<String, String> cfg = new HashMap<>();
        cfg.put(SinkConnectorConfig.NAME_CONFIG, name);
        cfg.put(SinkConnectorConfig.CONNECTOR_CLASS_CONFIG, Config.CONNECTOR_CLASS);
        cfg.put(SinkConnectorConfig.TOPICS_CONFIG, name);
        cfg.put(SinkConnectorConfig.TASKS_MAX_CONFIG, "2");
        cfg.put("key.converter.schemas.enable", "false");
        cfg.put("value.converter.schemas.enable", "false");
        cfg.put(ArangoSinkConfig.CONNECTION_ENDPOINTS, ArangoDbDeployment.getEndpoints());
        cfg.put(ArangoSinkConfig.CONNECTION_USER, "root");
        cfg.put(ArangoSinkConfig.CONNECTION_PASSWORD, "test");
        cfg.put(ArangoSinkConfig.CONNECTION_DATABASE, "_system");
        cfg.put(ArangoSinkConfig.CONNECTION_COLLECTION, name);
        return cfg;
    }

    @Override
    public void produce(Object key, Map<String, Object> value) {
        Object serKey = key != null ? serializeRecordKey(key) : null;
        Object serValue = value != null ? serializeRecordValue(value) : null;
        producer.send(new ProducerRecord<>(name, serKey, serValue));
        producer.flush();
    }

    @Override
    public void close() {
        producer.close();
        adminClient.close();
        collection.db().arango().shutdown();
    }

    public int getTopicPartitions() {
        return Config.TOPIC_PARTITIONS;
    }

    private ArangoCollection createCollection() {
        ArangoSinkConfig cfg = new ArangoSinkConfig(getConfig());
        ArangoCollection col = cfg.createCollection();
        if (col.exists()) {
            col.drop();
        }
        col.create();
        return col;
    }

    private AdminClient createAdminClient() {
        Properties adminClientConfig = new Properties();
        adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConnectDeployment.getInstance().getBootstrapServers());
        return AdminClient.create(adminClientConfig);
    }

    private void createTopic() throws ExecutionException, InterruptedException {
        Set<String> topics = adminClient.listTopics().names().get();
        if (topics.contains(name)) {
            adminClient.deleteTopics(Collections.singleton(name)).all().get();
        }
        adminClient.createTopics(Collections.singletonList(new NewTopic(name, getTopicPartitions(), Config.TOPIC_REPLICATION_FACTOR)))
                .all()
                .toCompletionStage()
                .handle((v, e) -> {
                    if (e == null || e instanceof TopicExistsException) {
                        return null;
                    } else {
                        throw new RuntimeException(e);
                    }
                })
                .toCompletableFuture()
                .get();
    }
}
