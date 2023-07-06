import java.util.Collection;
import java.util.Map;

public interface KafkaConnectOperations {
    void createConnector(Map<String, String> config);

    String getConnectorState(String name);

    Collection<String> getConnectors();

    void deleteConnector(String name);

}
