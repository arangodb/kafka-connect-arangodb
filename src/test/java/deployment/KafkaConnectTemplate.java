package deployment;

import org.sourcelab.kafka.connect.apiclient.Configuration;
import org.sourcelab.kafka.connect.apiclient.KafkaConnectClient;
import org.sourcelab.kafka.connect.apiclient.request.dto.NewConnectorDefinition;

import java.util.Collection;
import java.util.Map;

class KafkaConnectTemplate implements KafkaConnectOperations {
    private final KafkaConnectClient client;

    KafkaConnectTemplate(String kafkaConnectHost) {
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
        client.deleteConnector(name);
    }
}
