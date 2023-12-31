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
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
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

public class RecordConverter {
    private final static Logger LOG = LoggerFactory.getLogger(RecordConverter.class);

    private final JsonDeserializer deserializer;
    private final JsonConverter jsonConverter;
    private final KeyConverter keyConverter;

    public RecordConverter(KeyConverter keyConverter) {
        this.keyConverter = keyConverter;
        deserializer = new JsonDeserializer();
        jsonConverter = new JsonConverter();
        Map<String, Object> converterConfig = new HashMap<>();
        converterConfig.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
        converterConfig.put(JsonConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
        jsonConverter.configure(converterConfig);
    }

    public ObjectNode convert(SinkRecord record) {
        LOG.debug("Converting value for record: {}", record);
        byte[] bytes = jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
        JsonNode node = Optional.ofNullable(deserialize(bytes))
                .orElseGet(JsonNodeFactory.instance::objectNode);
        if (!node.isObject()) {
            throw new DataException("Record value cannot be read as JSON object: " + node.getClass().getName());
        }

        ObjectNode data = (ObjectNode) node;
        String keyFromField = KeyConverter.mapKey(data.get("_key"));
        String key = keyFromField != null ? keyFromField : keyConverter.convert(record);
        data.put("_key", key);
        LOG.debug("Converted record value: {}", data);
        return data;
    }

    private JsonNode deserialize(byte[] bytes) {
        try {
            return deserializer.deserialize(null, bytes);
        } catch (SerializationException e) {
            throw new DataException(e);
        }
    }

}
