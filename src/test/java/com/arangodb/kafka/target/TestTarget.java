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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;


public abstract class TestTarget implements Connector, Producer, Closeable {
    private final static Logger LOG = LoggerFactory.getLogger(TestTarget.class);

    private final String name;
    private final String dlqName;
    private KafkaProducer<Object, Object> producer;
    private KafkaConsumer<String, String> dlqConsumer;
    private ArangoCollection collection;
    private AdminClient adminClient;
    private final Map<String, ConsumerRecord<String, String>> dlqRecords;
    private ScheduledExecutorService dlqExecutor;

    public TestTarget(String name) {
        this.name = name;
        dlqName = name + "-DLQ";
        dlqRecords = new ConcurrentHashMap<>();
    }

    public void init() {
        producer = new KafkaProducer<>(producerConfig());
        collection = createCollection();
        adminClient = createAdminClient();
        try {
            createTopics();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        if (supportsDLQ()) {
            dlqExecutor = Executors.newSingleThreadScheduledExecutor();
            dlqExecutor.execute(() -> {
                dlqConsumer = new KafkaConsumer<>(dlqConsumerConfig());
                dlqConsumer.subscribe(Collections.singleton(dlqName));
            });
            dlqExecutor.scheduleAtFixedRate(this::consumeDlq, 50, 100, TimeUnit.MILLISECONDS);
        }
    }

    private void consumeDlq() {
        ConsumerRecords<String, String> consumerRecords = dlqConsumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, String> record : consumerRecords) {
            LOG.trace("received DLQ record: {}", record.key());
            dlqRecords.put(record.key(), record);
        }
        dlqConsumer.commitAsync();
    }

    @Override
    public String getName() {
        return name;
    }

    public Map<String, ConsumerRecord<String, String>> getDlqRecords() {
        return dlqRecords;
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

    private Map<String, Object> dlqConsumerConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConnectDeployment.getInstance().getBootstrapServers());
        cfg.put(ConsumerConfig.GROUP_ID_CONFIG, dlqName + UUID.randomUUID());
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        cfg.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        return cfg;
    }

    @Override
    public Map<String, String> getConfig() {
        Map<String, String> cfg = new HashMap<>();
        cfg.put(SinkConnectorConfig.NAME_CONFIG, name);
        cfg.put(SinkConnectorConfig.CONNECTOR_CLASS_CONFIG, Config.CONNECTOR_CLASS);
        cfg.put(SinkConnectorConfig.TOPICS_CONFIG, name);
        cfg.put(SinkConnectorConfig.TASKS_MAX_CONFIG, "2");
        cfg.put("errors.tolerance", "all");
        cfg.put("key.converter.schemas.enable", "false");
        cfg.put("value.converter.schemas.enable", "false");
        cfg.put(ArangoSinkConfig.CONNECTION_ENDPOINTS, ArangoDbDeployment.getEndpoints());
        cfg.put(ArangoSinkConfig.CONNECTION_USER, "root");
        cfg.put(ArangoSinkConfig.CONNECTION_PASSWORD, "test");
        cfg.put(ArangoSinkConfig.CONNECTION_DATABASE, "_system");
        cfg.put(ArangoSinkConfig.CONNECTION_COLLECTION, name);
        if (supportsDLQ()) {
            cfg.put(SinkConnectorConfig.DLQ_TOPIC_NAME_CONFIG, dlqName);
            cfg.put(SinkConnectorConfig.DLQ_CONTEXT_HEADERS_ENABLE_CONFIG, "true");
        }
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
        if (supportsDLQ()) {
            dlqExecutor.execute(() -> dlqConsumer.close());
            dlqExecutor.shutdown();
        }
        adminClient.close();
        collection.db().arango().shutdown();
    }

    public boolean supportsDLQ() {
        return false;
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

    private void createTopics() throws ExecutionException, InterruptedException {
        Set<String> topics = adminClient.listTopics().names().get();
        if (topics.contains(name)) {
            adminClient.deleteTopics(Collections.singleton(name)).all().get();
        }
        createTopic(name);

        if (supportsDLQ()) {
            if (topics.contains(dlqName)) {
                adminClient.deleteTopics(Collections.singleton(dlqName)).all().get();
            }
            createTopic(dlqName);
        }
    }

    private void createTopic(String topicName) throws ExecutionException, InterruptedException {
        adminClient.createTopics(Collections.singletonList(new NewTopic(topicName, getTopicPartitions(), Config.TOPIC_REPLICATION_FACTOR)))
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
