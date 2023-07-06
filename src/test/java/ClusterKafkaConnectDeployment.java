import java.util.Objects;

enum ClusterKafkaConnectDeployment implements KafkaConnectDeployment {
    INSTANCE;

    private final String kafkaBootstrapServers;

    ClusterKafkaConnectDeployment() {
        kafkaBootstrapServers = KafkaDeployment.getKafkaBootstrapServers();
        Objects.requireNonNull(kafkaBootstrapServers);
        assert !kafkaBootstrapServers.isEmpty();

        String kafkaConnectHost = KafkaConnectDeployment.getKafkaConnectHost();
        Objects.requireNonNull(kafkaConnectHost);
        assert !kafkaConnectHost.isEmpty();
    }

    @Override
    public void start() {
        // no-op
    }

    @Override
    public void stop() {
        // no-op
    }

    @Override
    public String getBootstrapServers() {
        return kafkaBootstrapServers;
    }

    @Override
    public KafkaConnectOperations client() {
        return new KafkaConnectTemplate(KafkaConnectDeployment.getKafkaConnectHost());
    }

}
