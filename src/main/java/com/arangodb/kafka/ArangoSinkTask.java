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

import com.arangodb.ArangoCollection;
import com.arangodb.entity.Permissions;
import com.arangodb.kafka.config.ArangoSinkConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class ArangoSinkTask extends SinkTask {
    private static final Logger LOG = LoggerFactory.getLogger(ArangoSinkTask.class);
    private ArangoCollection col;
    private ArangoWriter writer;

    @Override
    public String version() {
        return PackageVersion.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        LOG.info("starting ArangoSinkTask");

        ArangoSinkConfig config = new ArangoSinkConfig(props);
        LOG.info("task config: {}", config);
        col = config.createCollection();
        writer = new ArangoWriter(config, col, context);
        config.logUnused();

        testConnectivity();
        testPermissions(config.getUser());
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        writer.put(records);
    }

    @Override
    public void stop() {
        LOG.info("stopping ArangoSinkTask");
        if (col != null) {
            col.db().arango().shutdown();
        }
    }

    private void testConnectivity() {
        LOG.info("testing connectivity to ArangoDB");
        Exception lastException = null;
        for (int i = 0; i < 10; i++) {
            try {
                String version = col.db().getVersion().getVersion();
                LOG.info("Connected to ArangoDB: {}", version);
                return;
            } catch (Exception e) {
                LOG.warn("Got exception while testing connectivity to ArangoDB.", e);
                lastException = e;
            }
        }
        throw new ConnectException("Could not connect to ArangoDB.", lastException);
    }

    private void testPermissions(String user) {
        LOG.info("testing permissions to write ArangoDB");
        Permissions permissions = col.getPermissions(user);
        LOG.info("granted permissions: {}", permissions);
        if (!Permissions.RW.equals(permissions)) {
            throw new ConnectException("User [" + user + "] has no write permissions for target collection [" + col.name() + "]");
        }
    }
}
