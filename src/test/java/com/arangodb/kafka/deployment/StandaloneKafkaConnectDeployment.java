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

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.policy.AllConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;

import java.util.HashMap;
import java.util.Map;

enum StandaloneKafkaConnectDeployment implements KafkaConnectDeployment {
    INSTANCE;

    private final KafkaDeployment kafka = KafkaDeployment.getInstance();
    private Connect connect;

    @Override
    public void start() {
        kafka.start();

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

        AllConnectorClientConfigOverridePolicy allConnectorClientConfigOverridePolicy =
                new AllConnectorClientConfigOverridePolicy();

        Worker worker = new Worker(
                workerId, time, plugins, config, new MemoryOffsetBackingStore(),
                allConnectorClientConfigOverridePolicy);
        Herder herder = new StandaloneHerder(worker, "cluster-id", allConnectorClientConfigOverridePolicy);

        RestServer rest = new RestServer(config, null);
        rest.initializeServer();

        connect = new Connect(herder, rest);
        connect.start();
    }

    @Override
    public void stop() {
        connect.stop();
        kafka.stop();
        connect.awaitStop();
    }

    @Override
    public String getBootstrapServers() {
        return kafka.getBootstrapServers();
    }

    @Override
    public KafkaConnectOperations client() {
        return new KafkaConnectTemplate("http://127.0.0.1:8083");
    }

}
