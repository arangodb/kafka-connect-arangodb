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

import com.arangodb.config.HostDescription;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

enum ProxiedArangoDbDeployment implements ArangoDbDeployment {
    INSTANCE;

    private final String endpoints;
    private final List<ProxiedEndpoint> proxiedEndpoints;

    ProxiedArangoDbDeployment() {
        Logger LOG = LoggerFactory.getLogger(ProxiedArangoDbDeployment.class);
        endpoints = System.getProperty("arango.endpoints", "172.28.0.1:8529");
        LOG.info("Using arango.endpoints: {}", endpoints);
        assert !endpoints.isEmpty();

        ToxiproxyClient client = new ToxiproxyClient("127.0.0.1", 8474);

        proxiedEndpoints = new ArrayList<>();
        for (String e : endpoints.split(",")) {
            HostDescription hd = HostDescription.parse(e);
            HostDescription proxy = new HostDescription("127.0.0.1", 10_000 + hd.getPort());
            ProxiedEndpoint pe = new ProxiedEndpoint(e, proxy.getHost(), proxy.getPort(), e);
            Proxy p = client.getProxyOrNull(pe.getName());
            try {
                if (p != null) {
                    p.delete();
                }
                pe.setProxy(client.createProxy(pe.getName(), "127.0.0.1:" + pe.getPort(), pe.getUpstream()));
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
            proxiedEndpoints.add(pe);
        }
        LOG.info("Using arango.endpoints proxies: {}", getEndpoints());
    }

    @Override
    public String getEndpoints() {
        return proxiedEndpoints.stream()
                .map(it -> it.getHost() + ":" + it.getPort())
                .collect(Collectors.joining(","));
    }

    @Override
    public String getAdminEndpoints() {
        return endpoints;
    }

    @Override
    public List<ProxiedEndpoint> getProxiedEndpoints() {
        return proxiedEndpoints;
    }
}
