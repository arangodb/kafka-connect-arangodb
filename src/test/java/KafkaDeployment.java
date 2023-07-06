public interface KafkaDeployment {

    static KafkaDeployment getInstance() {
        return ExternalKafkaDeployment.INSTANCE;
    }

    static String getKafkaBootstrapServers() {
        return System.getProperty("kafka.bootstrap.servers");
    }

    void start();

    void stop();

    String getBootstrapServers();
}
