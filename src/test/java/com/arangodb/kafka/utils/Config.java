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

import com.arangodb.kafka.ArangoSinkConnector;

public class Config {
    public static final String ADB_HOST = "172.28.0.1";
    public static final int ADB_PORT = 8529;
    public static final String SCHEMA_REGISTRY_URL = "http://172.28.11.21:8081";
    public static final String TOPIC_NAME = "writeIT";
    public static final String COLLECTION_NAME = "writeIT";
    public static final String CONNECTOR_NAME = "my-connector";
    public static final String CONNECTOR_CLASS = ArangoSinkConnector.class.getName();

}
