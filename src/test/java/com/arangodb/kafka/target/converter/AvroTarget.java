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

import com.arangodb.kafka.deployment.SchemaRegistryDeployment;
import com.arangodb.kafka.target.TestTarget;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Map;

public class AvroTarget extends TestTarget {
    private static final Schema VALUE_RECORD_SCHEMA = new Schema.Parser()
            .parse("{"
                    + "  \"type\":\"record\","
                    + "  \"name\":\"record\","
                    + "  \"fields\": ["
                    + "    {\"name\":\"_key\",\"type\":[\"null\", \"string\"]},"
                    + "    {\"name\":\"foo\",\"type\":\"string\"}"
                    + "  ]"
                    + "}");

    public AvroTarget(String name) {
        super(name);
    }

    @Override
    public Map<String, Object> producerConfig() {
        Map<String, Object> cfg = super.producerConfig();
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        cfg.put("schema.registry.url", SchemaRegistryDeployment.getSchemaRegistryUrl());
        return cfg;
    }

    @Override
    public Map<String, String> getConfig() {
        Map<String, String> cfg = super.getConfig();
        cfg.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        cfg.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        cfg.put("value.converter.schema.registry.url", SchemaRegistryDeployment.getSchemaRegistryUrl());
        return cfg;
    }

    @Override
    public Object serializeRecordValue(Map<String, Object> data) {
        GenericData.Record value = new GenericData.Record(VALUE_RECORD_SCHEMA);
        data.forEach(value::put);
        return value;
    }

}
