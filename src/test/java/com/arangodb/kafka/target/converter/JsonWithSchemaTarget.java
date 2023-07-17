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

package com.arangodb.kafka.target.converter;

import com.arangodb.kafka.target.TestTarget;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;

import java.util.Map;

public class JsonWithSchemaTarget extends TestTarget {

    private final static String keySchema = "{\"schema\":{\"type\":\"string\",\"optional\":true}}";
    private final static String valueSchema =
            "{\"schema\": {" +
                    "      \"type\": \"struct\"," +
                    "      \"fields\": [" +
                    "          {" +
                    "              \"type\": \"string\"," +
                    "              \"optional\": true," +
                    "              \"field\": \"_key\"" +
                    "          }," +
                    "          {" +
                    "              \"type\": \"string\"," +
                    "              \"optional\": false," +
                    "              \"field\": \"foo\"" +
                    "          }" +
                    "      ]," +
                    "      \"optional\": false," +
                    "      \"name\": \"JsonWithSchemaTarget\"" +
                    "}}";

    private final ObjectMapper mapper = new ObjectMapper();

    public JsonWithSchemaTarget(String name) {
        super(name);
    }

    @Override
    public Map<String, Object> producerConfig() {
        Map<String, Object> cfg = super.producerConfig();
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return cfg;
    }

    @Override
    public Map<String, String> getConfig() {
        Map<String, String> cfg = super.getConfig();
        cfg.put(SinkConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        cfg.put(SinkConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        cfg.put("key.converter.schemas.enable", "true");
        cfg.put("value.converter.schemas.enable", "true");
        return cfg;
    }

    @Override
    public Object serializeRecordKey(String key) {
        try {
            JsonNode payload = mapper.convertValue(key, JsonNode.class);
            ObjectNode res = (ObjectNode) mapper.readTree(keySchema);
            return res.set("payload", payload);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object serializeRecordValue(Map<String, Object> data) {
        try {
            JsonNode payload = mapper.convertValue(data, JsonNode.class);
            ObjectNode res = (ObjectNode) mapper.readTree(valueSchema);
            return res.set("payload", payload);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
