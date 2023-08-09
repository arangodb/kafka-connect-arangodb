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

public class SchemaRegistryDeployment {

    public static String getSchemaRegistryUrlConnect() {
        Class<?> kafkaDeploymentClass = KafkaConnectDeployment.getInstance().getClass();
        if (kafkaDeploymentClass == StandaloneKafkaConnectDeployment.class) {
            return "http://172.28.0.1:8081";
        } else if (kafkaDeploymentClass == ClusterKafkaConnectDeployment.class) {
            return "http://schema-registry:8081";
        } else {
            throw new IllegalStateException("Unknown KafkaConnectDeployment class: " + kafkaDeploymentClass);
        }
    }

    public static String getSchemaRegistryUrlClient() {
        return "http://172.28.0.1:8081";
    }

}
