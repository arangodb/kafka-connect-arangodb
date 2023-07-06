import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.policy.AllConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;

import java.util.HashMap;
import java.util.Map;

enum StandaloneKafkaConnectDeployment implements KafkaConnectDeployment {
    INSTANCE;

    private final KafkaDeployment kafka = KafkaDeployment.getInstance();
    private Connect connect;

    @Override
    public void start() {
        kafka.start();

        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("bootstrap.servers", kafka.getBootstrapServers());
        workerProps.put("plugin.path", "target/classes");
        workerProps.put("offset.storage.file.filename", "");
        workerProps.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("key.converter.schemas.enable", "false");
        workerProps.put("value.converter.schemas.enable", "false");

        Time time = Time.SYSTEM;
        String workerId = "test-worker";

        Plugins plugins = new Plugins(workerProps);
        StandaloneConfig config = new StandaloneConfig(workerProps);

        AllConnectorClientConfigOverridePolicy allConnectorClientConfigOverridePolicy =
                new AllConnectorClientConfigOverridePolicy();

        Worker worker = new Worker(
                workerId, time, plugins, config, new MemoryOffsetBackingStore(),
                allConnectorClientConfigOverridePolicy);
        Herder herder = new StandaloneHerder(worker, "cluster-id", allConnectorClientConfigOverridePolicy);

        RestServer rest = new RestServer(config, null);
        rest.initializeServer();

        connect = new Connect(herder, rest);
        connect.start();
    }

    @Override
    public void stop() {
        connect.stop();
        kafka.stop();
        connect.awaitStop();
    }

    @Override
    public String getBootstrapServers() {
        return kafka.getBootstrapServers();
    }

    @Override
    public KafkaConnectOperations client() {
        return new KafkaConnectTemplate("http://127.0.0.1:8083");
    }

}
