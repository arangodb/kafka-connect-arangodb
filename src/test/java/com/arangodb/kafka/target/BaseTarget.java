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

import com.arangodb.kafka.config.ArangoSinkConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.storage.StringConverter;

import java.util.Map;

public class BaseTarget extends TestTarget {
    private final ObjectMapper mapper = new ObjectMapper();

    public BaseTarget(String name) {
        super(name);
    }

    @Override
    public Map<String, Object> producerConfig() {
        Map<String, Object> cfg = super.producerConfig();
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return cfg;
    }

    @Override
    public Map<String, String> getConfig() {
        Map<String, String> cfg = super.getConfig();
        cfg.put(SinkConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        cfg.put(SinkConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        cfg.put(ArangoSinkConfig.BATCH_SIZE, "3");
        return cfg;
    }

    @Override
    public Object serializeRecordValue(Map<String, Object> data) {
        return mapper.convertValue(data, JsonNode.class);
    }
}
