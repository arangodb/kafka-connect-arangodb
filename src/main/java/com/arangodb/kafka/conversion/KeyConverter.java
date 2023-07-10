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

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
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

public class KeyConverter {
    private final static Logger LOG = LoggerFactory.getLogger(KeyConverter.class);
    private final JsonDeserializer deserializer;
    private final JsonConverter jsonConverter;

    public KeyConverter() {
        deserializer = new JsonDeserializer();
        jsonConverter = new JsonConverter();
        Map<String, Object> converterConfig = new HashMap<>();
        converterConfig.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
        converterConfig.put(JsonConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
        jsonConverter.configure(converterConfig);
    }

    public String convert(SinkRecord record) {
        Object key = record.key();
        if (key == null) {
            String newKey = String.format("%s-%d-%d", record.topic(), record.kafkaPartition(), record.kafkaOffset());
            LOG.debug("Assigning _key: {}", newKey);
            return newKey;
        }
        if (String.valueOf(key).isEmpty()) {
            throw new DataException("Key is used as document id and can not be empty.");
        }

        Schema keySchema = record.keySchema();
        Schema.Type schemaType;
        if (keySchema == null) {
            schemaType = ConnectSchema.schemaType(key.getClass());
            if (schemaType == null) {
                throw new DataException(
                        "Java class " + key.getClass() + " does not have corresponding schema type."
                );
            }
        } else {
            schemaType = keySchema.type();
        }

        switch (schemaType) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case STRING:
                return String.valueOf(key);
            default:
                throw new DataException(schemaType.name() + " is not supported as the document id.");
        }

    }

}
