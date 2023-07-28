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

import com.arangodb.ArangoDB;
import com.arangodb.Request;
import com.arangodb.Response;
import com.arangodb.config.HostDescription;
import com.arangodb.kafka.config.ArangoSinkConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ArangoSinkConnector extends SinkConnector {
    private static final Logger LOG = LoggerFactory.getLogger(SinkConnector.class);
    private Map<String, String> config;
    private ArangoDB adb;
    private ScheduledExecutorService monitor;
    private boolean acquireHostList;
    private volatile Set<HostDescription> endpoints;

    @Override
    public String version() {
        return "1.0.0-SNAPSHOT";
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

        endpoints = sinkConfig.getEndpoints();
        acquireHostList = sinkConfig.getAcquireHostList();
        if (acquireHostList) {
            adb = sinkConfig.createMonitorClient();
            updateHostList();
            monitor = Executors.newSingleThreadScheduledExecutor();
            monitor.scheduleAtFixedRate(this::monitorHosts, ArangoSinkConfig.ACQUIRE_HOST_LIST_INTERVAL_MS,
                    ArangoSinkConfig.ACQUIRE_HOST_LIST_INTERVAL_MS, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ArangoSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        String taskEndpoints = endpoints.stream()
                .map(e -> e.getHost() + ":" + e.getPort())
                .collect(Collectors.joining(","));
        List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> taskCfg = new HashMap<>(config);
            taskCfg.put(ArangoSinkConfig.CONNECTION_ENDPOINTS, taskEndpoints);
            configs.add(taskCfg);
        }
        return configs;
    }

    @Override
    public void stop() {
        LOG.info("stopping ArangoSinkConnector");
        if (acquireHostList) {
            adb.shutdown();
            monitor.shutdown();
            try {
                if (!monitor.awaitTermination(ArangoSinkConfig.MONITOR_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                    monitor.shutdownNow();
                }
            } catch (InterruptedException ex) {
                monitor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public ConfigDef config() {
        return ArangoSinkConfig.CONFIG_DEF;
    }

    private Set<HostDescription> acquireHostList() {
        LOG.trace("acquiring host list");
        Request<?> request = Request.builder()
                .method(Request.Method.GET)
                .path("/_api/cluster/endpoints")
                .build();
        try {
            Response<ObjectNode> response = adb.execute(request, ObjectNode.class);
            return parseAcquireHostListResponse(response.getBody());
        } catch (Exception e) {
            LOG.warn("Got exception while acquiring the host list: ", e);
            return Collections.emptySet();
        }
    }

    private Set<HostDescription> parseAcquireHostListResponse(ObjectNode node) {
        Set<HostDescription> res = new HashSet<>();
        ArrayNode endpoints = (ArrayNode) node.get("endpoints");
        for (JsonNode endpoint : endpoints) {
            res.add(HostDescription.parse(endpoint.get("endpoint").textValue().replaceFirst(".*://", "")));
        }
        return res;
    }

    private boolean updateHostList() {
        Set<HostDescription> hosts = acquireHostList();
        if (!hosts.isEmpty() && !endpoints.equals(hosts)) {
            LOG.info("Detected change in the acquired host list: \n\t old: {} \n\t new: {}", endpoints, hosts);
            endpoints = hosts;
            return true;
        } else {
            return false;
        }
    }

    private void monitorHosts() {
        if (updateHostList()) {
            LOG.info("Requesting tasks reconfiguration.");
            context.requestTaskReconfiguration();
        }
    }

}
