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

package com.arangodb.kafka.deployment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaRegistryDeployment {
    private static final Logger LOG;
    private final static String schemaRegistryUrl;

    static {
        LOG = LoggerFactory.getLogger(SchemaRegistryDeployment.class);
        schemaRegistryUrl = System.getProperty("schema.registry.url", "http://127.0.0.1:8081");
        LOG.info("Using schema.registry.url: {}", schemaRegistryUrl);
        assert !schemaRegistryUrl.isEmpty();
    }

    public static String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }
}
