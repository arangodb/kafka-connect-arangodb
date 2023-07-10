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
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.ConverterType;

import java.util.HashMap;
import java.util.Map;

public class ValueConverter {

    private final JsonDeserializer deserializer;
    private final JsonConverter jsonConverter;
    private final KeyConverter keyConverter;

    public ValueConverter() {
        deserializer = new JsonDeserializer();
        jsonConverter = new JsonConverter();
        Map<String, Object> converterConfig = new HashMap<>();
        converterConfig.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
        converterConfig.put(JsonConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
        jsonConverter.configure(converterConfig);
        keyConverter = new KeyConverter();
    }

    public ObjectNode convert(SinkRecord record) {
        Object value = record.value();
        if (!(value instanceof Map) && !(value instanceof Struct)) {
            throw new DataException("Unsupported record value format: " + record.value().getClass());
        }

        JsonNode tree;
        try {
            byte[] bytes = jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
            tree = deserializer.deserialize(record.topic(), bytes);
        } catch (SerializationException e) {
            throw new DataException(e);
        }

        if (!(tree instanceof ObjectNode)) {
            throw new DataException("Record value cannot be read as JSON object");
        }

        ObjectNode data = (ObjectNode) tree;
        if (!data.hasNonNull("_key")) {
            String key = keyConverter.convert(record);
            data.put("_key", key);
        }

        return data;
    }

}
