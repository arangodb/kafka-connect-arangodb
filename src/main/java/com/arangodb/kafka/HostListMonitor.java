package com.arangodb.kafka;

import com.arangodb.ArangoDB;
import com.arangodb.Request;
import com.arangodb.Response;
import com.arangodb.config.HostDescription;
import com.arangodb.kafka.config.ArangoSinkConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HostListMonitor {
    private static final Logger LOG = LoggerFactory.getLogger(HostListMonitor.class);

    private final ArangoDB adb;
    private final ScheduledExecutorService es;
    private final ConnectorContext context;
    private final ArangoSinkConfig sinkConfig;
    private volatile Set<HostDescription> endpoints;

    public HostListMonitor(ArangoSinkConfig sinkConfig, ConnectorContext context) {
        this.sinkConfig = sinkConfig;
        endpoints = sinkConfig.getEndpoints();
        adb = sinkConfig.createMonitorClient();
        this.context = context;
        es = Executors.newSingleThreadScheduledExecutor();
    }

    void start() {
        updateHostList();
        int interval = sinkConfig.getAcquireHostIntervalMs();
        es.scheduleAtFixedRate(this::monitorHosts, interval, interval, TimeUnit.MILLISECONDS);
    }

    public Set<HostDescription> getEndpoints() {
        return endpoints;
    }

    public void stop() {
        adb.shutdown();
        es.shutdown();
        try {
            if (!es.awaitTermination(ArangoSinkConfig.MONITOR_REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                es.shutdownNow();
            }
        } catch (InterruptedException ex) {
            es.shutdownNow();
            Thread.currentThread().interrupt();
        }
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
