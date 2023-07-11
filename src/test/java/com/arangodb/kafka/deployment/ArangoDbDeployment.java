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

import com.arangodb.config.HostDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class ArangoDbDeployment {
    private static final Logger LOG;
    private final static String endpoints;
    private static final List<HostDescription> hosts;

    static {
        LOG = LoggerFactory.getLogger(ArangoDbDeployment.class);
        endpoints = System.getProperty("arango.endpoints", "172.28.0.1:8529");
        LOG.info("Using arango.endpoints: {}", endpoints);
        assert !endpoints.isEmpty();

        hosts = new ArrayList<>();
        for (String ep : endpoints.split(",")) {
            hosts.add(HostDescription.parse(ep));
        }
        assert hosts.size() > 0;
    }

    public static String getEndpoints() {
        return endpoints;
    }

    public static HostDescription getHost() {
        return hosts.get(0);
    }

}
