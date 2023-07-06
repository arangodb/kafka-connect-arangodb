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

package deployment;

import java.util.Objects;

enum ClusterKafkaConnectDeployment implements KafkaConnectDeployment {
    INSTANCE;

    private final String kafkaBootstrapServers;

    ClusterKafkaConnectDeployment() {
        kafkaBootstrapServers = KafkaDeployment.getKafkaBootstrapServers();
        Objects.requireNonNull(kafkaBootstrapServers);
        assert !kafkaBootstrapServers.isEmpty();

        String kafkaConnectHost = KafkaConnectDeployment.getKafkaConnectHost();
        Objects.requireNonNull(kafkaConnectHost);
        assert !kafkaConnectHost.isEmpty();
    }

    @Override
    public void start() {
        // no-op
    }

    @Override
    public void stop() {
        // no-op
    }

    @Override
    public String getBootstrapServers() {
        return kafkaBootstrapServers;
    }

    @Override
    public KafkaConnectOperations client() {
        return new KafkaConnectTemplate(KafkaConnectDeployment.getKafkaConnectHost());
    }

}
