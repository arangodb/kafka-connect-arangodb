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

import org.sourcelab.kafka.connect.apiclient.Configuration;
import org.sourcelab.kafka.connect.apiclient.KafkaConnectClient;
import org.sourcelab.kafka.connect.apiclient.request.dto.NewConnectorDefinition;
import org.sourcelab.kafka.connect.apiclient.rest.exceptions.InvalidRequestException;

import java.util.Collection;
import java.util.Map;

public class KafkaConnectTemplate implements KafkaConnectOperations {
    private final KafkaConnectClient client;

    public KafkaConnectTemplate(String kafkaConnectHost) {
        client = new KafkaConnectClient(new Configuration(kafkaConnectHost));
    }

    @Override
    public void createConnector(Map<String, String> config) {
        String name = config.get("name");
        if (name != null && client.getConnectors().contains(name)) {
            client.deleteConnector(name);
        }
        client.addConnector(NewConnectorDefinition.newBuilder()
                .withName(name)
                .withConfig(config)
                .build());
    }

    @Override
    public String getConnectorState(String name) {
        return client.getConnectorStatus(name).getConnector().get("state");
    }

    @Override
    public Collection<String> getConnectors() {
        return client.getConnectors();
    }

    @Override
    public void deleteConnector(String name) {
        try {
            client.deleteConnector(name);
        } catch (InvalidRequestException e) {
            // ignore
        }
    }
}
