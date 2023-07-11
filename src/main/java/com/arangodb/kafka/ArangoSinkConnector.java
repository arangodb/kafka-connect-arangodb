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

import com.arangodb.kafka.config.ArangoSinkConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ArangoSinkConnector extends SinkConnector {
    private static final Logger LOG = LoggerFactory.getLogger(SinkConnector.class);
    private Map<String, String> config;

    @Override
    public String version() {
        return "1.0.0-SNAPSHOT";
    }

    @Override
    public void start(Map<String, String> props) {
        LOG.info("starting connector");
        LOG.info("connector config: {}", props);
        this.config = props;

        try {
            // validation
            new ArangoSinkConfig(props);
        } catch (ConfigException e) {
            throw new ConnectException(e);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ArangoSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            configs.add(new HashMap<>(config));
        }
        return configs;
    }

    @Override
    public void stop() {
        LOG.info("stopping ArangoSinkConnector");
    }

    @Override
    public ConfigDef config() {
        return ArangoSinkConfig.CONFIG_DEF;
    }
}
