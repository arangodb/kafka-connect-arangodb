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

package com.arangodb.kafka.conversion;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.ConverterType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class KeyConverter {
    private final static Logger LOG = LoggerFactory.getLogger(KeyConverter.class);
    private final JsonDeserializer deserializer;
    private final JsonConverter jsonConverter;

    public KeyConverter() {
        deserializer = new JsonDeserializer();
        jsonConverter = new JsonConverter();
        Map<String, Object> converterConfig = new HashMap<>();
        converterConfig.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
        converterConfig.put(JsonConverterConfig.TYPE_CONFIG, ConverterType.KEY.getName());
        jsonConverter.configure(converterConfig);
    }

    public String convert(SinkRecord record) {
        LOG.debug("Extracting key for record: {}", record);
        byte[] bytes = jsonConverter.fromConnectData(record.topic(), record.keySchema(), record.key());
        JsonNode node = deserialize(bytes);
        String key = Optional.ofNullable(mapKey(node)).orElseGet(() ->
                String.format("%s-%d-%d", record.topic(), record.kafkaPartition(), record.kafkaOffset()));
        LOG.debug("Assigning _key: {}", key);
        return key;
    }

    public static String mapKey(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        } else if (node.isTextual()) {
            return node.textValue();
        } else if (node.isIntegralNumber()) {
            return String.valueOf(node.numberValue());
        } else {
            throw new DataException("Record key cannot be read as string: " + node.getClass().getName());
        }
    }

    private JsonNode deserialize(byte[] bytes) {
        try {
            return deserializer.deserialize(null, bytes);
        } catch (SerializationException e) {
            throw new DataException(e);
        }
    }

}
