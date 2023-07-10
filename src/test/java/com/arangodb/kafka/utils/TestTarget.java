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

package com.arangodb.kafka.utils;

import deployment.KafkaConnectDeployment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;

import java.util.HashMap;
import java.util.Map;

import static com.arangodb.kafka.utils.Config.*;


public abstract class TestTarget {

    private final KafkaProducer<Object, Object> producer;

    TestTarget() {
        producer = new KafkaProducer<>(producerConfig());
    }

    abstract Object serializeRecordValue(Map<String, Object> data);

    Map<String, Object> producerConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConnectDeployment.getInstance().getBootstrapServers());
        cfg.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        return cfg;
    }

    public Map<String, String> config() {
        Map<String, String> cfg = new HashMap<>();
        cfg.put(SinkConnectorConfig.NAME_CONFIG, CONNECTOR_NAME);
        cfg.put(SinkConnectorConfig.CONNECTOR_CLASS_CONFIG, CONNECTOR_CLASS);
        cfg.put(SinkConnectorConfig.TOPICS_CONFIG, TOPIC_NAME);
        cfg.put(SinkConnectorConfig.TASKS_MAX_CONFIG, "2");
        cfg.put("key.converter.schemas.enable", "false");
        cfg.put("value.converter.schemas.enable", "false");
        cfg.put("arango.host", ADB_HOST);
        cfg.put("arango.port", String.valueOf(ADB_PORT));
        cfg.put("arango.user", "root");
        cfg.put("arango.password", "test");
        cfg.put("arango.database", "_system");
        cfg.put("arango.collection", COLLECTION_NAME);
        return cfg;
    }

    public void produce(String key, Map<String, Object> value) {
        Object data = serializeRecordValue(value);
        producer.send(new ProducerRecord<>(TOPIC_NAME, key, data));
    }

    public void flush() {
        producer.flush();
    }

    public void close() {
        producer.close();
    }
}
