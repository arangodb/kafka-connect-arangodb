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

package spi;

import deployment.KafkaConnectDeployment;
import deployment.KafkaConnectOperations;
import deployment.KafkaConnectTemplate;
import deployment.KafkaDeployment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class ClusterKafkaConnectDeployment extends KafkaConnectDeployment {

    private static Logger LOG = LoggerFactory.getLogger(ClusterKafkaConnectDeployment.class);
    private final String kafkaBootstrapServers;
    private final String kafkaConnectHost;

    public ClusterKafkaConnectDeployment() {
        kafkaBootstrapServers = KafkaDeployment.getKafkaBootstrapServers();
        LOG.info("Using kafka.bootstrap.servers: {}", kafkaBootstrapServers);
        Objects.requireNonNull(kafkaBootstrapServers, "Required system property: kafka.bootstrap.servers");
        assert !kafkaBootstrapServers.isEmpty();

        kafkaConnectHost = getKafkaConnectHost();
        LOG.info("Using kafka.connect.host: {}", kafkaConnectHost);
        Objects.requireNonNull(kafkaConnectHost, "Required system property: kafka.connect.host");
        assert !kafkaConnectHost.isEmpty();
    }

    @Override
    public String getBootstrapServers() {
        return kafkaBootstrapServers;
    }

    @Override
    public KafkaConnectOperations client() {
        return new KafkaConnectTemplate(kafkaConnectHost);
    }

    @Override
    public String getSchemaRegistryUrlConnect() {
        return "http://schema-registry:8081";
    }

    @Override
    public void start() {
        // no-op
    }

}
