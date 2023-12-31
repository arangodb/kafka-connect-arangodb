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
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.policy.AllConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.ConnectRestServer;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class StandaloneKafkaConnectDeployment extends KafkaConnectDeployment {

    private final KafkaDeployment kafka = KafkaDeployment.getInstance();

    public StandaloneKafkaConnectDeployment() {
    }

    @Override
    public String getBootstrapServers() {
        return kafka.getBootstrapServers();
    }

    @Override
    public KafkaConnectOperations client() {
        return new KafkaConnectTemplate("http://127.0.0.1:8083");
    }

    @Override
    public String getSchemaRegistryUrlConnect() {
        return "http://172.28.0.1:8081";
    }

    @Override
    public void start() {
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("bootstrap.servers", kafka.getBootstrapServers());
        workerProps.put("plugin.path", "target/classes");
        workerProps.put("offset.storage.file.filename", "");
        workerProps.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("key.converter.schemas.enable", "false");
        workerProps.put("value.converter.schemas.enable", "false");

        Time time = Time.SYSTEM;
        String workerId = "test-worker";

        Plugins plugins = new Plugins(workerProps);
        StandaloneConfig config = new StandaloneConfig(workerProps);
        AllConnectorClientConfigOverridePolicy allowOverride = new AllConnectorClientConfigOverridePolicy();
        MemoryOffsetBackingStore offsetStore = new MemoryOffsetBackingStore() {
            @Override
            public Set<Map<String, Object>> connectorPartitions(String connectorName) {
                throw new UnsupportedOperationException();
            }
        };
        Worker worker = new Worker(workerId, time, plugins, config, offsetStore, allowOverride);
        Herder herder = new StandaloneHerder(worker, "cluster-id", allowOverride);
        RestClient restClient = new RestClient(config);
        ConnectRestServer restServer = new ConnectRestServer(null, restClient, workerProps);
        restServer.initializeServer();

        new Connect(herder, restServer).start();
    }

}
