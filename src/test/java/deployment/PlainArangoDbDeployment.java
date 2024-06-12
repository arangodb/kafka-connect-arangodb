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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

enum PlainArangoDbDeployment implements ArangoDbDeployment {
    INSTANCE;

    private final String endpoints;

    PlainArangoDbDeployment() {
        Logger LOG = LoggerFactory.getLogger(PlainArangoDbDeployment.class);

        String topology = System.getProperty("arango.topology", "single");
        String defaultEndpoints;
        switch (topology) {
            case "single":
                defaultEndpoints = "172.28.0.1:8529";
                break;
            case "cluster":
                defaultEndpoints = "172.28.0.1:8529,172.28.0.1:8539,172.28.0.1:8549";
                break;
            default:
                throw new IllegalArgumentException("Invalid arango.topology: " + topology);
        }

        endpoints = System.getProperty("arango.endpoints", defaultEndpoints);
        LOG.info("Using arango.endpoints: {}", endpoints);
        assert !endpoints.isEmpty();
    }

    @Override
    public String getEndpoints() {
        return endpoints;
    }

}
