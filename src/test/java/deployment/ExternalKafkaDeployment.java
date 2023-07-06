package deployment;

import java.util.Objects;

enum ExternalKafkaDeployment implements KafkaDeployment {
    INSTANCE;

    ExternalKafkaDeployment() {
        String kafkaBootstrapServers = getBootstrapServers();
        Objects.requireNonNull(kafkaBootstrapServers, "Required system property: kafka.bootstrap.servers");
        assert !kafkaBootstrapServers.isEmpty();
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
        return KafkaDeployment.getKafkaBootstrapServers();
    }

}
