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

package com.arangodb.kafka;

import com.arangodb.config.HostDescription;
import com.arangodb.kafka.config.ArangoSinkConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class ArangoSinkConnector extends SinkConnector {
    private static final Logger LOG = LoggerFactory.getLogger(SinkConnector.class);
    private Map<String, String> config;
    private boolean acquireHostList;
    private List<HostDescription> initialEndpoints;
    private HostListMonitor hostListMonitor;

    @Override
    public String version() {
        return PackageVersion.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        LOG.info("starting connector");
        LOG.info("connector config: {}", props);
        this.config = props;

        ArangoSinkConfig sinkConfig;
        try {
            sinkConfig = new ArangoSinkConfig(props);
        } catch (ConfigException e) {
            throw new ConnectException(e);
        }

        acquireHostList = sinkConfig.isAcquireHostListEnabled();
        initialEndpoints = sinkConfig.getEndpoints();

        if (acquireHostList) {
            hostListMonitor = new HostListMonitor(sinkConfig, context);
            hostListMonitor.start();
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ArangoSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<HostDescription> endpoints = new ArrayList<>(acquireHostList ? hostListMonitor.getEndpoints() : initialEndpoints);
        int rotationDistance = endpoints.size() / maxTasks;
        if (rotationDistance == 0) {
            rotationDistance = 1;
        }

        List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            Collections.rotate(endpoints, rotationDistance);
            String taskEndpoints = endpoints.stream()
                    .map(e -> e.getHost() + ":" + e.getPort())
                    .collect(Collectors.joining(","));
            Map<String, String> taskCfg = new HashMap<>(config);
            taskCfg.put(ArangoSinkConfig.CONNECTION_ENDPOINTS, taskEndpoints);
            configs.add(taskCfg);
            LOG.info("task #{} config: {}", i, new ArangoSinkConfig(taskCfg));
        }
        return configs;
    }

    @Override
    public void stop() {
        if (acquireHostList) {
            LOG.info("stopping ArangoSinkConnector");
            hostListMonitor.stop();
        }
    }

    @Override
    public ConfigDef config() {
        return ArangoSinkConfig.CONFIG_DEF;
    }

}
